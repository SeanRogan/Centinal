# THIS REPO IS DEPRECATED
# FOR THE UPDATED VERSION SEE: https://github.com/seanrogan/centinel

## Centinal - Market Data Streaming Service

A Spring Boot application that streams real-time market data from Coinbase to TimescaleDB using WebSocket connections.

### Features

- Real-time market data streaming from Coinbase WebSocket API
- TimescaleDB integration for time-series data storage
- Health monitoring and metrics
- Configurable symbol subscriptions
- Error handling and logging

### Prerequisites

- Java 21
- Maven 3.6+
- PostgreSQL with TimescaleDB extension
- Docker (optional, for containerized deployment)

### Quick Start

#### 1. Database Setup

First, ensure you have PostgreSQL with TimescaleDB extension installed:

```sql
-- Create database
CREATE DATABASE centinal;

-- Connect to the database
\c centinal

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create hypertable for market data (will be created automatically by JPA)
-- The application will create the table with proper time-series optimization
```

#### 2. Configuration

Update the database connection in `src/main/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/centinal
    username: your_username
    password: your_password
```

#### 3. Run the Application

```bash
# Build the application
mvn clean compile

# Run the application
mvn spring-boot:run
```

The application will automatically:
- Connect to the database
- Start streaming market data from Coinbase
- Create the necessary database tables
- Begin processing and storing market data

### Configuration

#### Market Data Symbols

Configure which symbols to stream in `application.yml`:

```yaml
market:
  data:
    enabled: true
    symbols: BTC-USD,ETH-USD,ADA-USD,SOL-USD
```

### Actuator Endpoints
```
GET /actuator/health
GET /actuator/info
GET /actuator/metrics
```

## Database Schema

The application creates a `market_data` table with the following structure:

- `id`: Primary key
- `timestamp`: Time of the data point
- `symbol`: Trading pair (e.g., BTC-USD)
- `exchange`: Exchange name (coinbase)
- `price`: Current price
- `volume`: 24h volume
- `bid`: Best bid price
- `ask`: Best ask price
- `high_24h`: 24h high
- `low_24h`: 24h low
- `open_24h`: 24h open price
- `raw_data`: Raw JSON data from exchange
- `created_at`: Record creation timestamp

## Architecture

### Components

1. **CoinbaseWebsocketClient**: Handles WebSocket connection to Coinbase
2. **MarketDataStreamingService**: Processes incoming messages and persists to database
3. **MarketDataRepository**: JPA repository for database operations

### Data Flow

1. Application starts and connects to Coinbase WebSocket
2. Subscribes to configured symbols (ticker channel)
3. Receives real-time market data messages
4. Processes and validates the data
5. Persists to TimescaleDB with proper indexing

## Monitoring

The application includes:

- Health checks via Spring Actuator
- Detailed logging for debugging
- Metrics for monitoring performance
- Error handling and recovery

## Development

### Running Tests

```bash
mvn test
```

### Building Docker Image

```bash
mvn clean package
docker build -t centinal .
```

### Docker Compose

Use the provided `compose.yaml` for local development:

```bash
docker-compose up -d
```

## Troubleshooting

### Common Issues

1. **Database Connection**: Ensure PostgreSQL is running and accessible
2. **WebSocket Connection**: Check network connectivity to Coinbase
3. **Memory Usage**: Monitor heap usage for high-frequency data
4. **Database Performance**: Consider TimescaleDB compression policies

### Logs

Check application logs for detailed information:

```bash
tail -f logs/application.log
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.
