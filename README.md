# AgenticMDM - Using Databricks Multi Agent

## Overview
The Chief Data Officer of Northbridge Financial, a multi-asset investment management firm, is building an Agentic MDM to unify market and security master records from FactSet, Moody's, and Bloomberg. Between 2025-08-18 and 2025-09-12, a surge of conflicting equity identifiers (CUSIP/ISIN/SEDOL) and sub-product mappings (share classes, ADRs) increased duplicate and mismatched master entries by ~2.8x, notably for EMEA ADRs and ETFs. A dated vendor taxonomy update (Bloomberg ticker schema 2025-08-19) and Moody's sector reclassification (2025-08-25) misaligned with Northbridge rules, leading to lineage breaks and commentary disputes. The Agentic MDM resolves matches via golden record consolidation, standardizes identifiers, preserves lineage, and exposes commentary trails. Dashboards show the anomaly in master data quality, the mix driving it, the dated root cause, and business impact via downstream reporting accuracy and risk exposure.

![alt text](https://github.com/saswata30/Agentic_MDM/edit/main/README.md#:~:text=Agentic_MDM_Dashboard_1?raw=true)
![alt text](https://github.com/saswata30/Agentic_MDM/edit/main/README.md#:~:text=Agentic_MDM_Dashboard_1?raw=true)

## Deployment

This bundle can be deployed to any Databricks workspace using Databricks Asset Bundles (DAB):

### Prerequisites
1. **Databricks CLI**: Install the latest version
   ```bash
   pip install databricks-cli
   ```
2. **Authentication**: Configure your workspace credentials
   ```bash
   databricks configure
   ```
3. **Workspace Access**: Ensure you have permissions for:
   - Unity Catalog catalog/schema creation
   - SQL Warehouse access
   - Workspace file storage

### Deploy the Bundle
```bash
# Navigate to the dab directory
cd dab/

# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace (--force-lock to override any existing locks)
databricks bundle deploy --force-lock

# Run the data generation workflow
databricks bundle run demo_workflow
```

The deployment will:
1. Create Unity Catalog resources (schema and volume)
2. Upload PDF files to workspace (if applicable)
3. Deploy job and dashboard resources

The workflow will:
1. Create Unity Catalog catalog if it doesn't exist (DAB doesn't support catalog creation)
2. Generate synthetic data using Faker and write to Unity Catalog Volume
3. Execute SQL transformations (bronze → silver → gold)
4. Deploy agent bricks (Genie spaces, Knowledge Assistants, Multi-Agent Supervisors) if configured

## Bundle Contents

### Core Files
- `databricks.yml` - Asset bundle configuration defining jobs, dashboards, and deployment settings
- `bricks_conf.json` - Agent brick configurations (Genie/KA/MAS) if applicable
- `agent_bricks_service.py` - Service for managing agent brick resources (includes type definitions)
- `deploy_resources.py` - Script to recreate agent bricks in the target workspace

### Data Generation
- Python scripts using Faker library for realistic synthetic data
- Configurable row counts, schemas, and business logic
- Automatic Delta table creation in Unity Catalog

### SQL Transformations
- `transformations.sql` - SQL transformations for data processing
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) medallion architecture
- Views and tables for business analytics

### Agent Bricks
This bundle includes AI agent resources:

- **Genie Space** 
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

### Dashboards
This bundle includes Lakeview dashboards:
- **MDM Quality, Lineage, and Exposure Impact** - Business intelligence dashboard with visualizations

### PDF Documents
This bundle includes PDF documents that will be uploaded to the workspace:
- PDF files are automatically uploaded during bundle deployment via DAB artifacts
- These PDFs are included in the bundle workspace path
- You can manually copy them to Unity Catalog Volumes if needed for RAG scenarios
- Example: Use Databricks Files API or `dbutils.fs.cp` to move files to a volume

## Configuration

### Unity Catalog
- **Catalog**: `change to your catalog`
- **Schema**: `change to your schema`
- **Workspace Path**: `change to your path`

### Customization
You can modify the bundle by editing `databricks.yml`:
- Change target catalog/schema in the `variables` section
- Adjust cluster specifications for data generation
- Add additional tasks or resources

## Key Questions This Demo Answers
1. When did master data quality degrade, and what was the magnitude of identifier conflicts versus baseline?
2. Which regions and sub-products contributed most to duplicates and mapping disputes, and how did lineage completeness move during the event?
3. Which dated vendor changes align with the onset and recovery, and how strong is the temporal correlation with spikes in conflicts?
4. How many golden records were created post-fix, and to what extent did they reduce open conflicts and increase lineage completeness?
5. What is the estimated misclassified exposure by region and sector during 2025-08-20..2025-09-12, and when did it fall below materiality thresholds?
6. Which securities required steward overrides, what were the commentary rationales, and how did those decisions affect downstream reporting?

## Deployment to New Workspaces

This bundle is **portable** and can be deployed to any Databricks workspace:

1. The bundle will recreate all resources in the target workspace
2. Agent bricks (Genie/KA/MAS) are recreated from saved configurations in `bricks_conf.json`
3. SQL transformations and data generation scripts are environment-agnostic
4. Dashboards are deployed as Lakeview dashboard definitions

Simply run `databricks bundle deploy` in any workspace where you have the required permissions.

## Troubleshooting

### Common Issues

**Bundle validation fails:**
- Ensure `databricks.yml` has valid YAML syntax
- Check that catalog and schema names are valid
- Verify warehouse lookup matches an existing warehouse

**Agent brick deployment fails:**
- Check that `bricks_conf.json` exists and contains valid configurations
- Ensure you have permissions to create Genie spaces, KA tiles, and MAS tiles
- Verify vector search endpoint exists for Knowledge Assistants

**SQL transformations fail:**
- Ensure the catalog and schema exist in the target workspace
- Check warehouse permissions and availability
- Review SQL syntax for Unity Catalog compatibility (3-level namespace: `catalog.schema.table`)

### Getting Help
- Review Databricks Asset Bundles documentation: https://docs.databricks.com/dev-tools/bundles/
- Check the generated code in this bundle for implementation details
- Contact your Databricks workspace administrator for permissions issues



**Created**: 2025-11-06 15:37:45
**User**: saswata.sengupta@databricks.com
