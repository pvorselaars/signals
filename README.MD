# Signals

**Signals** is a self-contained **.NET 9 Blazor** application that acts as a lightweight OpenTelemetry backend. It directly **receives OTLP/gRPC telemetry signals**, stores them in a **SQLite database**, and visualizes them through a built-in **UI**.

It uses the official [**OpenTelemetry protobuf definitions**](https://github.com/open-telemetry/opentelemetry-proto) for decoding and processing telemetry data, with no need for an external collector.

End-to-end observability in a single app. All in one process — no collector, no external storage, no dependencies:

* **Receives telemetry** (traces, metrics, logs) over OTLP/gRPC
* **Parses OTLP messages** using the official **OpenTelemetry proto files**
* **Persists data** with **SQLite**
* **Visualizes telemetry** via a modern Blazor UI
* Supports any OTLP-compatible client (.NET, Python, Go, etc.)

## 🚀 Getting Started

### Prerequisites

* [.NET 10 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/10.0)
* Optional: OTLP-compatible client (e.g., app with OpenTelemetry SDK)

### Run the App

```bash
git clone https://github.com/pvorselaars/signals
cd signals

git submodule update --init

dotnet run 
```

* Web UI: `https://localhost:443`
* OTLP/gRPC Endpoint: `http://localhost:4317`

### Send Telemetry

**Signals** sends telemetry to itself but also accepts telemetry from other sources.

## 🧬 Protobuf Integration

Signals uses the **OpenTelemetry Protocol (OTLP) protobuf definitions** to:

* Implement the `TraceService`, `MetricsService`, and `LogsService` gRPC interfaces
* Deserialize incoming telemetry payloads directly from the wire

The `.proto` files are compiled into C# classes during build using `Grpc.Tools`.

## 🔒 Security

Signals is currently usable for **local development and prototyping**. gRPC endpoints are unauthenticated by default. For production or shared environments, you should:

* Restrict access with reverse proxies or firewalls
* Implement gRPC-level authentication (e.g., mTLS or auth interceptors)

## ✅ Roadmap

- [ ] Span visualizer
- [ ] Alerts
- [x] Metrics
- [x] Logs

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.

## 🤝 Contributing

Contributions, suggestions, and issues are welcome. Please open a PR or file an issue in GitHub.

## 📬 Contact

Questions? Reach out on GitHub or submit an issue.