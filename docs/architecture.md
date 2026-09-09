# Architecture note

For this assignment I used a simple three-layer setup. The idea is to keep the raw source separate from the cleaned data and the final data product.

```text
       ArcGIS FeatureServer
               |
               | Python + requests
               v
       +-------------------+
       | Bronze            |
       | raw API response  |
       | + run id          |
       +---------+---------+
                 |
                 | PySpark
                 v
       +-------------------+
       | Silver            |
       | typed + filtered  |
       | DQ + dedupe       |
       +---------+---------+
                 |
          +------+------+
          |             |
          |             |
       bad coords     valid/warned
          |             |
          v             v
   +-------------+  +-------------+
   | Quarantine  |  | Gold v1     |
   | DQ-Q1       |  | data product|
   +-------------+  +-------------+
```

## Why I chose this

**Bronze** keeps the API response mostly as it came from the source. I added an ingestion run ID so that each run can be identified.

**Silver** is where I do the main transformations. The nested API response is flattened, coordinates are converted to doubles, the state filter is applied, and the DQ rules are checked.

**Quarantine** is separate from Silver/Gold because an invalid coordinate should not be published.

**Gold** contains only the columns that a consumer needs. The Gold path is versioned as `v1` so a breaking schema change can be published separately later.

## Azure approach

The local folders are intended to represent ADLS-style paths. In Azure, I would use ADLS Gen2 for storage and Databricks/Fabric/Synapse Spark for the transformations.

I did not add Terraform, CI/CD or scheduling because those are outside the scope of the assignment.
