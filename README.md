# earthquake

## Архитектура проекта

```mermaid
flowchart LR
    subgraph API
        API_E["Earthquake API"]
    end

    subgraph ETL
        AirFlow
    end

    subgraph Storage
        S3
    end

    subgraph DWH
        subgraph PostgreSQL
            subgraph model
                ods["ODS Layer"]
                dm["Data Mart Layer"]
            end
        end
    end

    subgraph BI
        MetaBase
    end

    API_E -->|Extract Data| AirFlow
    AirFlow -->|Load Data| S3
    S3 -->|Extract Data| AirFlow
    AirFlow -->|Load Data to ODS| ods
    ods -->|Extract Data| AirFlow
    AirFlow -->|Transform and Load Data to DM| dm
    dm -->|Visualize Data| MetaBase
```
