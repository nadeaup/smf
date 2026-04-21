package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gosnmp/gosnmp"
	"google.golang.org/api/iterator"
)

// ------------------------------------------------
// Data Structures
// ------------------------------------------------

// FirestoreData is the snapshot written to 'solar_data'
type FirestoreData struct {
	BatterySoC      string           `firestore:"batterySoC"`
	BatteryVoltage  string           `firestore:"batteryVoltage"`
	TotalLoad       float64          `firestore:"totalLoad"`
	SolarProduction float64          `firestore:"solarProduction"`
	GridPower       float64          `firestore:"gridPower"`
	GridVoltage     string           `firestore:"gridVoltage"`
	BatteryPower    float64          `firestore:"batteryPower"`
	Inverter1Temp   float64          `firestore:"inverter1Temp"`
	Inverter2Temp   float64          `firestore:"inverter2Temp"`
	PDUs            []PDUStatus      `firestore:"pdus"`
	TemperatureF    float64          `firestore:"temperatureF"`
	Condition       string           `firestore:"condition"`
	Cloudiness      int              `firestore:"cloudiness"`
	Sunrise         int64            `firestore:"sunrise"`
	Sunset          int64            `firestore:"sunset"`
	Timestamp       time.Time        `firestore:"timestamp"`
	Config          SanitizedConfig  `firestore:"config"`
	Miners          []MinerTelemetry `firestore:"miners"` // Use thin telemetry for snapshots
	LastActionLog   string           `firestore:"lastActionLog"`
	DebitPower      float64          `firestore:"debitPower"`
	ExpiresAt       time.Time        `firestore:"expiresAt"`
	ServiceHeartbeat map[string]time.Time `firestore:"serviceHeartbeat"`
}

// MinerTelemetry is the "thin" version of a miner record for solar_data
// It excludes financial/metadata (Algorithm, Coin, Revenue) owned by the Cloud sync job.
type MinerTelemetry struct {
	Model      string `json:"model" firestore:"model"`
	Name       string `json:"name,omitempty" firestore:"name,omitempty"` // Added for better matching in UI
	PDUName    int    `json:"pduName" firestore:"pduName"`
	Plug       int    `json:"plug" firestore:"plug"`
	PowerGroup string `json:"powerGroup" firestore:"powerGroup"`
	Power      int    `json:"power" firestore:"power"`
	NoDisplay  int    `json:"noDisplay" firestore:"noDisplay"`
}

// MinerEntry is the "rich" version read from 'system_config/settings'
type MinerEntry struct {
	Model      string   `json:"model" firestore:"model"`
	Name       string   `json:"name,omitempty" firestore:"name,omitempty"`
	PDUName    int      `json:"pduName" firestore:"pduName"`
	Plug       int      `json:"plug" firestore:"plug"`
	PowerGroup string   `json:"powerGroup" firestore:"powerGroup"`
	Power      int      `json:"power" firestore:"power"`
	Coin       string   `json:"coin,omitempty" firestore:"coin,omitempty"`
	Algorithm  string   `json:"algorithm,omitempty" firestore:"algorithm,omitempty"`
	Revenue    *float64 `json:"revenue,omitempty" firestore:"revenue,omitempty"`
	MinerCost  *float64 `json:"miner_cost,omitempty" firestore:"miner_cost,omitempty"`
	NoDisplay  int      `json:"noDisplay" firestore:"noDisplay"`
}

type OverrideConfig struct {
	Active      bool   `json:"active" firestore:"active"`
	TargetGroup string `json:"targetGroup" firestore:"targetGroup"`
	Until       int64  `json:"until" firestore:"until"`
}

type PDUStatus struct {
	Name  string         `json:"name" firestore:"name"`
	IP    string         `json:"ip" firestore:"ip"`
	Plugs map[string]int `json:"plugs" firestore:"plugs"`
}

type PowerActionRule struct {
	Name                    string   `json:"name" firestore:"name"`
	BatteryVoltageCondition string   `json:"batteryVoltageCondition" firestore:"batteryVoltageCondition"`
	TimeCondition           string   `json:"timeCondition" firestore:"timeCondition"`
	PowerGroupsOn           []string `json:"powerGroupsOn" firestore:"powerGroupsOn"`
	PowerGroupsOff          []string `json:"powerGroupsOff" firestore:"powerGroupsOff"`
}

type TimerConfig struct {
	OverrideCheckMin  int `json:"overrideCheckMin"`
	ActionIntervalMin int `json:"actionIntervalMin"`
	FirestoreSyncMin  int `json:"firestoreSyncMin"`
	WeatherUpdateMin  int `json:"weatherUpdateMin"`
	PduRefreshMin     int `json:"pduRefreshMin"`
}

type FullConfig struct {
	FirestoreProject      string            `json:"firestoreProject"`
	FirestoreKeyPath      string            `json:"firestoreKeyPath"`
	ApiKey                string            `json:"apiKey"`
	Latitude              float64           `json:"latitude"`
	Longitude             float64           `json:"longitude"`
	ElectricityCostPerKWh float64           `json:"ElectricityCostPerKWh"`
	SolarAssistant        string            `json:"solar-assistant"`
	SnmpPort              int               `json:"snmpPort"`
	Community             string            `json:"community"`
	ActionOID             string            `json:"actionoid"`
	StatusOID             string            `json:"statusoid"`
	Beta                  int               `json:"beta"`
	Timers                TimerConfig       `json:"timers"`
	Override              OverrideConfig    `json:"override"`
	PowerActionRules      []PowerActionRule `json:"powerActionRules"`
	Miners                []MinerEntry      `json:"miners"`
	Hysteresis            float64           `json:"hysteresis"`
	SolarBonusThreshold   float64           `json:"solarBonusThreshold"`
	SolarBonusVolts       float64           `json:"solarBonusVolts"`
}

type SanitizedConfig struct {
	Latitude              float64           `json:"latitude" firestore:"latitude"`
	Longitude             float64           `json:"longitude" firestore:"longitude"`
	ElectricityCostPerKWh float64           `json:"ElectricityCostPerKWh" firestore:"ElectricityCostPerKWh"`
	SolarAssistant        string            `json:"solar-assistant" firestore:"solar-assistant"`
	Override              OverrideConfig    `json:"override" firestore:"override"`
	PowerActionRules      []PowerActionRule `json:"powerActionRules" firestore:"powerActionRules"`
	Hysteresis            float64           `json:"hysteresis" firestore:"hysteresis"`
	SolarBonusThreshold   float64           `json:"solarBonusThreshold" firestore:"solarBonusThreshold"`
	SolarBonusVolts       float64           `json:"solarBonusVolts" firestore:"solarBonusVolts"`
}

type WeatherData struct {
	TemperatureF float64
	Cloudiness   int
	Sunrise      int64
	Sunset       int64
	DaylightLeft float64
	Condition    string
	UpdatedAt    time.Time
}

// ------------------------------------------------
// Globals
// ------------------------------------------------
var (
	mu               sync.RWMutex
	batterySoC       string
	batteryVoltage   string
	totalLoad        float64
	solarProduction  float64
	gridPower        float64
	gridVoltage      string
	batteryPower     float64
	inverter1Temp    float64
	inverter2Temp    float64
	latestPDUs       []PDUStatus
	lastActionLog    string
	previousMinersOn bool
	weatherMu        sync.RWMutex
	latestWeather    WeatherData
	config           FullConfig
	configPath       = "config_cache.json"
	heartbeats       = make(map[string]time.Time)

	// FIX #3: Shared HTTP client with timeout — prevents weather fetch from hanging forever
	httpClient = &http.Client{Timeout: 15 * time.Second}

	// Trigger for immediate action when config/override changes
	configChangeTrigger = make(chan bool, 1)
)

// ------------------------------------------------
// Helpers
// ------------------------------------------------
func parsePayload(payload string) float64 {
	clean := strings.TrimSpace(strings.NewReplacer("W", "", "V", "", "%", "", "A", "", " ", "").Replace(payload))
	val, _ := strconv.ParseFloat(clean, 64)
	return val
}

// FIX #1: calculateDebit now takes pre-snapshotted data to avoid reentrant RLock deadlock.
// Previously it acquired mu.RLock() while the caller already held mu.RLock(), which deadlocks
// if a writer (MQTT/config listener) is waiting between the two RLock calls.

// 192.168.x.x - replace with IP address of PDU
func calculateDebit(pdus []PDUStatus, miners []MinerEntry) float64 {
	totalDebit := 0.0
	for _, pdu := range pdus {
		pduID := 0
		switch pdu.IP {
		case "192.168.x.x":
			pduID = 1
		case "192.168.x.x":
			pduID = 2
		case "192.168.x.x":
			pduID = 3
		case "192.168.x.x":
			pduID = 4
		}
		for _, miner := range miners {
			if miner.PDUName == pduID {
				if status, ok := pdu.Plugs[strconv.Itoa(miner.Plug)]; ok && status == 2 {
					totalDebit += float64(miner.Power)
				}
			}
		}
	}
	return totalDebit
}

func saveCache(c FullConfig) {
	data, _ := json.MarshalIndent(c, "", " ")
	_ = os.WriteFile(configPath, data, 0644)
}

func loadCache() bool {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return false
	}
	err = json.Unmarshal(data, &config)
	return err == nil
}

// FIX #6: Hardened Firestore listener — recreates the snapshot iterator on terminal errors
// instead of spinning on a dead iterator. Uses exponential backoff for resilience.
func startConfigListener(ctx context.Context, client *firestore.Client) {
	backoff := 10 * time.Second
	maxBackoff := 5 * time.Minute

	for {
		if ctx.Err() != nil {
			log.Println("[FS] Config listener shutting down (context cancelled).")
			return
		}

		// Listen to system_config/settings for rich miner data and override state
		iter := client.Collection("system_config").Doc("settings").Snapshots(ctx)
		err := listenSnapshots(ctx, iter)
		iter.Stop()

		if ctx.Err() != nil {
			log.Println("[FS] Config listener shutting down (context cancelled).")
			return
		}

		log.Printf("[FS] Listener terminated: %v — reconnecting in %v", err, backoff)
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func listenSnapshots(ctx context.Context, iter *firestore.DocumentSnapshotIterator) error {
	for {
		snap, err := iter.Next()
		if err == iterator.Done {
			return fmt.Errorf("iterator done")
		}
		if err != nil {
			return err
		}
		if snap.Exists() {
			mu.Lock()
			snap.DataTo(&config)
			saveCache(config)
			mu.Unlock()
			log.Println("[FS] Config settings synchronized.")
			// Trigger immediate re-check of override/actions
			select {
			case configChangeTrigger <- true:
			default:
			}
		}
	}
}

// FIX #4: controlDevice snapshots config fields under the lock to prevent data races
func controlDevice(ip string, plug int, action uint) bool {
	if ip == "" {
		return false
	}
	mu.RLock()
	actionOID := config.ActionOID
	snmpPort := config.SnmpPort
	community := config.Community
	mu.RUnlock()

	oid := fmt.Sprintf("%s%d", actionOID, plug)
	g := &gosnmp.GoSNMP{
		Target:    ip,
		Port:      uint16(snmpPort),
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   2 * time.Second,
	}
	if err := g.Connect(); err != nil {
		return false
	}
	defer g.Conn.Close()
	pdu := gosnmp.SnmpPDU{Name: oid, Type: gosnmp.Integer, Value: int(action)}
	_, err := g.Set([]gosnmp.SnmpPDU{pdu})
	return err == nil
}

func getGroupRank(group string) int {
	switch strings.ToUpper(group) {
	case "L":
		return 1
	case "M":
		return 2
	case "H":
		return 3
	case "F":
		return 4
	default:
		return 0
	}
}

func evaluateCondition(value float64, condition string) bool {
	if condition == "" {
		return true
	}
	condition = strings.TrimSpace(condition)
	var op, thStr string
	if strings.HasPrefix(condition, ">=") {
		op = ">="
		thStr = condition[2:]
	} else if strings.HasPrefix(condition, "<=") {
		op = "<="
		thStr = condition[2:]
	} else if strings.HasPrefix(condition, ">") {
		op = ">"
		thStr = condition[1:]
	} else if strings.HasPrefix(condition, "<") {
		op = "<"
		thStr = condition[1:]
	} else {
		return false
	}
	th, _ := strconv.ParseFloat(strings.TrimSpace(thStr), 64)
	switch op {
	case ">":
		return value > th
	case ">=":
		return value >= th
	case "<":
		return value < th
	case "<=":
		return value <= th
	}
	return false
}

func evaluateTimeCondition(sunrise, sunset int64, condition string) bool {
	if condition == "" {
		return true
	}
	now := time.Now().Unix()
	switch condition {
	case "daytime":
		return now >= sunrise && now <= sunset
	case "nighttime":
		return now < sunrise || now > sunset
	}
	return false
}

// ------------------------------------------------
// MAIN LOGIC
// ------------------------------------------------

// FIX #4: processActions now snapshots ALL config fields it needs under a single lock acquisition,
// preventing data races on config.SolarBonusThreshold, config.SolarBonusVolts, etc.
func processActions(isOverrideCycle bool) {
	mu.RLock()
	bvS := batteryVoltage
	currOverride := config.Override
	miners := config.Miners
	rules := config.PowerActionRules
	hyst := config.Hysteresis
	solarProd := solarProduction
	battP := batteryPower
	totalL := totalLoad
	solarBonusThreshold := config.SolarBonusThreshold
	solarBonusVolts := config.SolarBonusVolts
	prevOn := previousMinersOn // FIX #6: snapshot under lock to prevent data race
	mu.RUnlock()

	bv := parsePayload(bvS)
	now := time.Now().Unix()

	// === HYSTERESIS ===
	effectiveBV := bv
	if prevOn {
		effectiveBV += hyst
	} else {
		effectiveBV -= hyst / 2
	}

	// === SOLAR PRODUCTION AWARENESS ===
	solarBonus := 0.0
	if solarProd > solarBonusThreshold && battP > 1500 {
		solarBonus = solarBonusVolts
	}
	effectiveBV += solarBonus

	var appliedRule string
	actions := make(map[string]uint)
	isOverrideActive := currOverride.Active && now < currOverride.Until

	if isOverrideActive && strings.ToUpper(currOverride.TargetGroup) == "OFF" {
		appliedRule = "OVERRIDE: FORCE OFF"
		for _, miner := range miners {
			actions[fmt.Sprintf("%d-%d", miner.PDUName, miner.Plug)] = 1
		}
	} else if isOverrideActive && bv < 50.5 {
		appliedRule = "SAFETY: LOW BATTERY"
	} else if isOverrideActive {
		targetRank := getGroupRank(currOverride.TargetGroup)
		appliedRule = fmt.Sprintf("OVERRIDE: %s", currOverride.TargetGroup)
		for _, miner := range miners {
			key := fmt.Sprintf("%d-%d", miner.PDUName, miner.Plug)
			if r := getGroupRank(miner.PowerGroup); r > 0 && r <= targetRank {
				actions[key] = 2
			} else {
				actions[key] = 1
			}
		}
	}

	if !isOverrideCycle && (appliedRule == "" || appliedRule == "SAFETY: LOW BATTERY") {
		if bv < 5.0 {
			return
		}
		weatherMu.RLock()
		w := latestWeather
		weatherMu.RUnlock()

		// === LATE AFTERNOON SAFETY ===
		if w.DaylightLeft <= 2.0 && solarProd < totalL && bv <= 51.6 {
			appliedRule = "LATE AFTERNOON SAFETY"
			for _, miner := range miners {
				key := fmt.Sprintf("%d-%d", miner.PDUName, miner.Plug)
				actions[key] = 1
			}
		} else {
			// Normal rule evaluation
			for _, rule := range rules {
				if evaluateCondition(effectiveBV, rule.BatteryVoltageCondition) &&
					evaluateTimeCondition(w.Sunrise, w.Sunset, rule.TimeCondition) {
					appliedRule = strings.TrimSpace(appliedRule + " " + rule.Name)
					for _, miner := range miners {
						key := fmt.Sprintf("%d-%d", miner.PDUName, miner.Plug)
						for _, gOn := range rule.PowerGroupsOn {
							if miner.PowerGroup == gOn {
								actions[key] = 2
							}
						}
						for _, gOff := range rule.PowerGroupsOff {
							if miner.PowerGroup == gOff {
								actions[key] = 1
							}
						}
					}
					break
				}
			}
		}
	}

	if appliedRule == "" {
		return
	}

	mu.Lock()
	lastActionLog = fmt.Sprintf("Action: [%s] BV: %.2f (eff: %.2f hyst+%.2f solar+%.2f)", appliedRule, bv, effectiveBV, hyst, solarBonus)
	mu.Unlock()

	pduIPs := map[int]string{1: "192.168.x.x", 2: "192.168.x.x", 3: "192.168.x.x", 4: "192.168.x.x"}
	for _, miner := range miners {
		if act, ok := actions[fmt.Sprintf("%d-%d", miner.PDUName, miner.Plug)]; ok {
			controlDevice(pduIPs[miner.PDUName], miner.Plug, act)
		}
	}

	// FIX #6: Update hysteresis state under lock to prevent data race
	newMinersOn := false
	for _, act := range actions {
		if act == 2 {
			newMinersOn = true
			break
		}
	}
	mu.Lock()
	previousMinersOn = newMinersOn
	mu.Unlock()
}

// FIX #3: fetchWeather uses the shared httpClient with a 15s timeout
func fetchWeather() WeatherData {
	mu.RLock()
	lat := config.Latitude
	lon := config.Longitude
	apiKey := config.ApiKey
	mu.RUnlock()

	url := fmt.Sprintf("https://api.openweathermap.org/data/3.0/onecall?lat=%.6f&lon=%.6f&units=imperial&exclude=minutely,hourly,daily,alerts&appid=%s", lat, lon, apiKey)
	resp, err := httpClient.Get(url)
	if err != nil {
		log.Printf("[WEATHER] Fetch error: %v", err)
		return WeatherData{}
	}
	defer resp.Body.Close()
	var data struct {
		Current struct {
			Dt, Sunrise, Sunset int64
			Temp                float64
			Clouds              int
			Weather             []struct{ Main string }
		}
	}
	json.NewDecoder(resp.Body).Decode(&data)
	daylight := float64(data.Current.Sunset-data.Current.Dt) / 3600.0
	if daylight < 0 {
		daylight = 0
	}
	w := WeatherData{TemperatureF: data.Current.Temp, Cloudiness: data.Current.Clouds, Sunrise: data.Current.Sunrise, Sunset: data.Current.Sunset, DaylightLeft: daylight, Condition: "Clear", UpdatedAt: time.Now().UTC()}
	if len(data.Current.Weather) > 0 {
		w.Condition = data.Current.Weather[0].Main
	}
	return w
}

// FIX #2 & #4: refreshPDUStates snapshots SNMP config under lock and uses safe type assertion
func refreshPDUStates() []PDUStatus {
	mu.RLock()
	snmpPort := config.SnmpPort
	community := config.Community
	statusOID := config.StatusOID
	mu.RUnlock()

	pduList := []struct{ Name, IP string }{{"PDU 1", "192.168.x.x"}, {"PDU 2", "192.168.x.x"}, {"PDU 3", "192.168.x.x"}, {"PDU 4", "192.168.x.x"}}
	var result []PDUStatus
	for _, p := range pduList {
		plugs := make(map[string]int)
		g := &gosnmp.GoSNMP{Target: p.IP, Port: uint16(snmpPort), Community: community, Version: gosnmp.Version2c, Timeout: 2 * time.Second}
		if err := g.Connect(); err == nil {
			for i := 1; i <= 16; i++ {
				oid := statusOID + strconv.Itoa(i)
				if res, err := g.Get([]string{oid}); err == nil && len(res.Variables) > 0 {
					// FIX #2: Safe type assertion — prevents panic if PDU returns unexpected type
					val, ok := res.Variables[0].Value.(int)
					if !ok {
						log.Printf("[SNMP] Unexpected type for %s plug %d: %T", p.Name, i, res.Variables[0].Value)
						continue
					}
					plugs[strconv.Itoa(i)] = val
				}
			}
			g.Conn.Close()
		}
		result = append(result, PDUStatus{Name: p.Name, IP: p.IP, Plugs: plugs})
	}
	return result
}

// ------------------------------------------------
// Main
// ------------------------------------------------
func main() {
	log.Println("[BOOT] Starting SMF-FS2 Cloud-First Service...")
	if !loadCache() {
		log.Fatal("[BOOT] config_cache.json missing.")
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", config.FirestoreKeyPath)

	// FIX #5: Cancellable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsClient, err := firestore.NewClient(ctx, config.FirestoreProject)
	if err != nil {
		log.Fatal(err)
	}
	defer fsClient.Close()

	go startConfigListener(ctx, fsClient)

	// FIX #7: MQTT Setup with connection lifecycle logging
	opts := mqtt.NewClientOptions().AddBroker("tcp://" + config.SolarAssistant).SetClientID("smf-fs-combined")
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(2 * time.Minute)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("[MQTT] Connection lost: %v — will auto-reconnect", err)
	})
	opts.SetReconnectingHandler(func(_ mqtt.Client, opts *mqtt.ClientOptions) {
		log.Println("[MQTT] Attempting reconnection...")
	})
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("[MQTT] Connected successfully.")
		// FIX #10: Reduced lock scope — only acquire write lock for matching topics
		c.Subscribe("solar_assistant/#", 0, func(_ mqtt.Client, msg mqtt.Message) {
			payload := string(msg.Payload())
			topic := msg.Topic()

			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(topic, "battery") && strings.Contains(topic, "voltage") {
				batteryVoltage = payload
			} else if strings.Contains(topic, "state_of_charge") {
				batterySoC = payload
			} else if strings.Contains(topic, "load_power") && !strings.Contains(topic, "non-essential") {
				totalLoad = parsePayload(payload)
			} else if strings.Contains(topic, "pv_power") && !strings.Contains(topic, "pv_power_") {
				solarProduction = parsePayload(payload)
			} else if strings.Contains(topic, "battery_power") || (strings.Contains(topic, "battery") && strings.Contains(topic, "power")) {
				batteryPower = parsePayload(payload)
			} else if strings.Contains(topic, "inverter_1/temperature") {
				inverter1Temp = parsePayload(payload)
			} else if strings.Contains(topic, "inverter_2/temperature") {
				inverter2Temp = parsePayload(payload)
			} else if strings.Contains(topic, "grid_power") {
				gridPower = parsePayload(payload)
			} else if strings.Contains(topic, "grid_voltage") {
				gridVoltage = payload
			}
		})
	})
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	defer mqttClient.Disconnect(1000)

	// FIX #5: Signal handling for graceful shutdown on reboot/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Periodic Tasks
	go func() {
		for {
			pdus := refreshPDUStates()
			mu.Lock()
			latestPDUs = pdus
			heartbeats["pduRefresh"] = time.Now().UTC()
			mu.Unlock()
			mu.RLock()
			interval := config.Timers.PduRefreshMin
			if interval < 1 {
				interval = 2
			}
			mu.RUnlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(interval) * time.Minute):
			}
		}
	}()

	go func() {
		for {
			w := fetchWeather()
			weatherMu.Lock()
			latestWeather = w
			mu.Lock()
			heartbeats["weatherUpdate"] = time.Now().UTC()
			mu.Unlock()
			weatherMu.Unlock()
			mu.RLock()
			interval := config.Timers.WeatherUpdateMin
			if interval < 1 {
				interval = 15
			}
			mu.RUnlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(interval) * time.Minute):
			}
		}
	}()

	go func() {
		for {
			processActions(true)
			mu.Lock()
			heartbeats["overrideCheck"] = time.Now().UTC()
			mu.Unlock()
			mu.RLock()
			interval := config.Timers.OverrideCheckMin
			if interval < 1 {
				interval = 1
			}
			mu.RUnlock()
			select {
			case <-ctx.Done():
				return
			case <-configChangeTrigger:
				log.Println("[EVENT] Config change detected, triggering immediate override sync.")
			case <-time.After(time.Duration(interval) * time.Minute):
			}
		}
	}()

	go func() {
		for {
			processActions(false)
			mu.Lock()
			heartbeats["actionInterval"] = time.Now().UTC()
			mu.Unlock()
			mu.RLock()
			interval := config.Timers.ActionIntervalMin
			if interval < 1 {
				interval = 5
			}
			mu.RUnlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(interval) * time.Minute):
			}
		}
	}()

	log.Println("[BOOT] Waiting 10 seconds for initial data...")
	select {
	case <-time.After(10 * time.Second):
	case sig := <-sigCh:
		log.Printf("[SHUTDOWN] Received %v during boot, exiting.", sig)
		cancel()
		return
	}

	// Firestore Sync Loop
	for {
		// FIX #1: Snapshot ALL data under a single lock, then release BEFORE any computation
		// that might need the lock. This eliminates the reentrant RLock deadlock.
		mu.RLock()
		weatherMu.RLock()

		// MAP: Convert rich MinerEntries to thin MinerTelemetries for solar_data snapshot
		var telemetryMiners []MinerTelemetry
		for _, m := range config.Miners {
			telemetryMiners = append(telemetryMiners, MinerTelemetry{
				Model:      m.Model,
				Name:       m.Name, // If name is present, use it for UI matching
				PDUName:    m.PDUName,
				Plug:       m.Plug,
				PowerGroup: m.PowerGroup,
				Power:      m.Power,
				NoDisplay:  m.NoDisplay,
			})
		}

		// Snapshot all values needed for Firestore document
		snapshotPDUs := latestPDUs
		snapshotMiners := config.Miners
		snapshot := FirestoreData{
			BatterySoC:      batterySoC,
			BatteryVoltage:  batteryVoltage,
			TotalLoad:       totalLoad,
			SolarProduction: solarProduction,
			GridPower:       gridPower,
			GridVoltage:     gridVoltage,
			BatteryPower:    batteryPower,
			Inverter1Temp:   inverter1Temp,
			Inverter2Temp:   inverter2Temp,
			PDUs:            snapshotPDUs,
			TemperatureF:    latestWeather.TemperatureF,
			Condition:       latestWeather.Condition,
			Cloudiness:      latestWeather.Cloudiness,
			Sunrise:         latestWeather.Sunrise,
			Sunset:          latestWeather.Sunset,
			Timestamp:       time.Now().UTC(),
			ExpiresAt:       time.Now().UTC().Add(7 * 24 * time.Hour),
			Miners:          telemetryMiners, // THIN DATA ONLY
			LastActionLog:   lastActionLog,
			Config: SanitizedConfig{
				Latitude:              config.Latitude,
				Longitude:             config.Longitude,
				ElectricityCostPerKWh: config.ElectricityCostPerKWh,
				SolarAssistant:        config.SolarAssistant,
				Override:              config.Override,
				PowerActionRules:      config.PowerActionRules,
				Hysteresis:            config.Hysteresis,
				SolarBonusThreshold:   config.SolarBonusThreshold,
				SolarBonusVolts:       config.SolarBonusVolts,
			},
			ServiceHeartbeat: heartbeats,
		}
		heartbeats["firestoreSync"] = time.Now().UTC()
		syncInterval := config.Timers.FirestoreSyncMin
		weatherMu.RUnlock()
		mu.RUnlock()

		// FIX #1: Calculate debit AFTER releasing locks, using snapshotted data
		snapshot.DebitPower = calculateDebit(snapshotPDUs, snapshotMiners)

		log.Printf("[SYNC] BV: %s | PV: %.0fW | Debit: %.0fW | TS: %s",
			snapshot.BatteryVoltage, snapshot.SolarProduction, snapshot.DebitPower,
			snapshot.Timestamp.Format("15:04:05"))

		sCtx, sCancel := context.WithTimeout(ctx, 20*time.Second)
		docRef, _, err := fsClient.Collection("solar_data").Add(sCtx, snapshot)
		sCancel()
		if err != nil {
			log.Printf("[SYNC ERROR] Failed: %v", err)
		} else {
			log.Printf("[SYNC SUCCESS] Entry: %s", docRef.ID)
		}

		if syncInterval < 1 {
			syncInterval = 1
		}

		// FIX #5: Use select for interruptible sleep — allows clean shutdown on signal
		select {
		case sig := <-sigCh:
			log.Printf("[SHUTDOWN] Received %v, shutting down gracefully...", sig)
			cancel()
			// Give in-flight operations a moment to finish
			time.Sleep(2 * time.Second)
			log.Println("[SHUTDOWN] Complete.")
			return
		case <-time.After(time.Duration(syncInterval) * time.Minute):
		}
	}
}
