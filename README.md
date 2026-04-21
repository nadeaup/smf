smf-fs2.go is the core controller of the Solar Mining Farm (SMF) system. It acts as a high-performance bridge between local power hardware (Inverters/PDUs) and the Google Cloud backend, automating power-switching decisions to optimize cryptocurrency mining based on real-time solar availability.

🚀 Main Features
Real-Time Solar Monitoring: Subscribes to local MQTT streams (via Solar Assistant) to track critical metrics: battery voltage, State of Charge (SoC), solar production, and total load.

Automated Load Management: Dynamically toggles mining hardware using SNMP commands to PDUs.

Power Group Logic: Uses a tiered system (Low, Medium, High, Full) to prioritize hardware based on energy overhead.

Intelligent Logic Engine:

Hysteresis: Prevents "flapping" (rapid power cycling) when battery voltage oscillates near a threshold.

Solar Awareness: Applies a "bonus" to effective voltage during peak production, enabling aggressive mining windows.

Late Afternoon Safety: Predictive shutdown protocols to ensure the battery bank maintains sufficient charge for overnight operations.

Cloud-Native Configuration: Listens to a Firestore document (system_config/settings) for live updates. Rules, miner assignments, and manual overrides can be triggered from the Admin Hub without a service restart.

Weather-Driven Decisions: Integrates with the OpenWeatherMap API to factor in sunset/sunrise times and cloud cover forecasts.

State Snapshots: Periodically pushes full system state snapshots to Firestore (solar_data), enabling real-time dashboards and long-term historical analysis via BigQuery.

🛠️ Technology Stack
Component	Technology	Purpose
Runtime	Go (Golang)	High-concurrency handling of MQTT, SNMP, and Cloud Sync.
Local Comms	MQTT	Real-time telemetry ingestion from Inverters.
Hardware Ctrl	SNMP	Power state manipulation for CyberPower/Generic PDUs.
Database	Firestore	Remote "Source of Truth" for config and state snapshots.
Auth	Google SDK	Secure authentication via Application Default Credentials.
External Data	OpenWeatherMap	Environmental data for predictive scheduling.
Persistence	JSON	Local config_cache.json for offline boot resilience.

Prerequisites
To run the SMF controller, the following environment must be present:
Solar Assistant: Configured and broadcasting MQTT telemetry. (gethers battery voltages, solar, batter and grid statsi from inverters etc)
PDU Hardware: SNMP-enabled PDUs (e.g., CyberPower) on the local network.
Google Cloud Project: Active Firestore instance with service account credentials.
Local Config: A valid config_cache.json for initial bootstrapping.
