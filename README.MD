# Signals

**Signals** is a self-contained **.NET 9 Blazor** application that acts as a lightweight OpenTelemetry backend. It directly **receives OTLP/gRPC telemetry signals**, stores them in a **SQLite database** using **Entity Framework Core**, and visualizes them through a built-in **UI**.

It uses the official [**OpenTelemetry protobuf definitions**](https://github.com/open-telemetry/opentelemetry-proto) for decoding and processing telemetry data, with no need for an external collector.

End-to-end observability in a single app:

* 📥 **Receives telemetry** (traces, metrics, logs) over OTLP/gRPC
* 🧠 **Parses OTLP messages** using the official **OpenTelemetry proto files**
* 💾 **Persists data** with **SQLite** + **EF Core**
* 🖥️ **Visualizes telemetry** via a modern Blazor UI
* 🎨 Uses [**Pico.css**](https://picocss.com/) for modern, minimal styling without JavaScript

All in one process — no collector, no external storage, no dependencies.

* ✅ Built-in **OTLP/gRPC receiver**
* 🧬 Uses **OpenTelemetry protobufs** directly (no intermediary SDKs)
* 📦 Stores data via **Entity Framework Core + SQLite**
* 🖼️ Blazor UI styled with **Pico.css** for exploring traces, logs, and metrics
* 🧪 Supports any OTLP-compatible client (.NET, Python, Go, etc.)
* 🛠️ Powered by **.NET 9**

## 🚀 Getting Started

### Prerequisites

* [.NET 9 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/9.0)
* OTLP-compatible client (e.g., app with OpenTelemetry SDK)

### Run the App

```bash
dotnet run 
```

* Web UI: `https://localhost:443`
* OTLP/gRPC Endpoint: `http://localhost:4317`

### Send Telemetry

**Signals** sends telemetry to itself but also accepts telemetry from other sources.

## 🧬 Protobuf Integration

Signals uses the **OpenTelemetry Protocol (OTLP) protobuf definitions** to:

* Implement the `TraceService`, `MetricsService`*, and `LogsService` gRPC interfaces (see roadmap)
* Deserialize incoming telemetry payloads directly from the wire
* Convert raw proto messages into internal EF Core models

The `.proto` files are compiled into C# classes during build using `Grpc.Tools`.

## 💾 Data Storage

* Database: **SQLite** (portable, file-based)
* ORM: **Entity Framework Core (Code-First)**
* Schema auto-created or updated via migrations

Database path: `./signals.db` 

## 🔒 Security

Signals is currently usable for **local development and prototyping**. gRPC endpoints are unauthenticated by default. For production or shared environments, you should:

* Restrict access with reverse proxies or firewalls
* Implement gRPC-level authentication (e.g., mTLS or auth interceptors)

## ✅ Roadmap

- [ ] Span visualizer
- [ ] Alerts
- [ ] Metrics
- [ ] Logs

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.

## 🤝 Contributing

Contributions, suggestions, and issues are welcome. Please open a PR or file an issue in GitHub.

## 📬 Contact

Questions? Reach out on GitHub or submit an issue.