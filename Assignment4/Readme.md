# Azure End-to-End Data Pipeline Assignment
### Celebal Technologies Internship | 2025

---

## 📋 Assignment Objective

Build an end-to-end data pipeline on Microsoft Azure using:
- **Azure Storage Account** (Blob Storage) — as data source
- **Azure Data Factory (ADF)** — for pipeline orchestration
- **IAM Roles** — for access management
- **Dataset**: Superstore Sales CSV from Kaggle

---

## ✅ Completion Status

| # | Task | Status |
|---|------|--------|
| 1 | Create Resource Group | ✅ Completed |
| 2 | Create Storage Account + Blob Containers | ⚠️ Documented* |
| 3 | Upload Superstore CSV to Blob | ⚠️ Documented* |
| 4 | Create Azure Data Factory | ⚠️ Documented* |
| 5 | Linked Services and Datasets | ⚠️ Documented* |
| 6 | Build Pipeline (Get Metadata + Copy Data) | ⚠️ Documented* |
| 7 | Execute and Monitor Pipeline | ⚠️ Documented* |
| 8 | Assign IAM Roles | ⚠️ Documented* |
| 9 | End-to-End Pipeline with If Condition | ⚠️ Documented* |

> **\* Documented** = Step fully explained with exact configurations and expected output.
> Could not be executed due to Azure for Students subscription policy restriction.
> **Error**: `RequestDisallowedByAzure` — "This policy maintains a set of best available regions
> where your subscription can deploy resources."
> Regions tried: Central India, East US, West US, East US 2, West Europe, North Europe,
> Southeast Asia, UK South — all returned the same error.

---

## Step 1 — Create Resource Group ✅

A Resource Group is a logical container for all Azure resources in a project.

### Configuration Used

| Field | Value |
|-------|-------|
| Subscription | Azure for Students |
| Resource Group Name | `rg-celebal-pipeline` |
| Region | Central India |

### Steps Performed
1. Opened `portal.azure.com` → signed in
2. Searched **Resource Groups** in the top bar → clicked it
3. Clicked **+ Create**
4. Filled name: `rg-celebal-pipeline` | Region: `Central India`
5. Clicked **Review + Create** → **Create**
6. ✅ Received notification: *Resource group created*

> 📸 **Screenshot 1**: Resource Group overview page — name, subscription, region visible

---

## Step 2 — Create Storage Account ⚠️

> **Error hit here**: `RequestDisallowedByAzure` — subscription policy blocked all regions

Azure Blob Storage stores files (CSVs, images, logs) in the cloud. It is the **SOURCE** in this pipeline.

### Configuration

| Field | Value |
|-------|-------|
| Resource Group | `rg-celebal-pipeline` |
| Storage Account Name | `celebalstorage25abc` |
| Region | East US (or any permitted region) |
| Performance | Standard |
| Redundancy | Locally Redundant Storage (LRS) |
| Primary Service | Azure Blob Storage |

### Blob Containers to Create

| Container | Purpose |
|-----------|---------|
| `input-data` | SOURCE — stores the Superstore CSV |
| `output-data` | DESTINATION — receives copied output CSV |

### Key Concepts
- **Storage Account** = parent resource (like a cloud hard drive)
- **Blob Container** = folder inside the storage account
- **Blob** = actual file stored (CSV, image, log, etc.)
- **LRS** = 3 copies within one datacenter — cheapest option

---

## Step 3 — Upload Superstore CSV ⚠️

### Dataset Details

| Property | Value |
|----------|-------|
| Dataset | Superstore Sales Dataset |
| Source | https://www.kaggle.com/datasets/vivek468/superstore-dataset-final |
| File Name | `Sample - Superstore.csv` |
| File Size | ~1 MB |
| Total Rows | 9,994 rows |
| Columns | 21 (Order ID, Ship Mode, Segment, Region, Category, Sales, Profit, etc.) |

### Upload Steps
1. Storage Account → **Containers** → click `input-data`
2. Click **Upload** in top bar
3. Browse → select `Sample - Superstore.csv`
4. Click **Upload** — file appears within seconds

> ⚠️ File name is **case-sensitive** in Blob Storage. Must match exactly in ADF Dataset config.

---

## Step 4 — Create Azure Data Factory ⚠️

ADF is Azure's cloud **ETL service** — moves and transforms data between sources using visual pipelines.

### Configuration

| Field | Value |
|-------|-------|
| Resource Group | `rg-celebal-pipeline` |
| Name | `adf-celebal-2025` |
| Region | Same as Storage Account |
| Version | V2 |
| Git Configuration | Configure Git later |

### ADF Studio Sections

| Section | Purpose |
|---------|---------|
| ✏️ **Author** | Create Pipelines, Datasets, Linked Services — main workspace |
| 📊 **Monitor** | View run history, status (Succeeded/Failed), activity duration |
| 🔧 **Manage** | Configure Linked Services, Integration Runtimes, triggers |
| 🏠 **Home** | Dashboard with recent activity |

---

## Step 5 — Linked Services & Datasets ⚠️

### What is a Linked Service?
A connection definition — tells ADF **HOW** to connect to a data store. Like a connection string.

### Linked Service Configuration

| Field | Value |
|-------|-------|
| Name | `ls_blob_storage` |
| Type | Azure Blob Storage |
| Authentication | Account Key (auto-filled from subscription) |
| Storage Account | `celebalstorage25abc` |
| Test Connection | Should return: *Connection successful* |

### Source Dataset — `ds_source_superstore`

| Field | Value |
|-------|-------|
| Linked Service | `ls_blob_storage` |
| Type | DelimitedText (CSV) |
| Container | `input-data` |
| File Name | `Sample - Superstore.csv` |
| First Row as Header | ✅ Yes |
| Import Schema | From connection/store |

### Destination Dataset — `ds_destination_output`

| Field | Value |
|-------|-------|
| Linked Service | `ls_blob_storage` |
| Type | DelimitedText (CSV) |
| Container | `output-data` |
| File Name | `superstore_output.csv` |
| First Row as Header | ✅ Yes |

---

## Step 6 — Build the Pipeline ⚠️

Pipeline name: `pl_superstore_copy`

### Architecture

```
Blob Storage        Get Metadata         Copy Data
(input-data)  -->   (Validate file)  -->  (input → output)
```

### Get Metadata Activity

| Field | Value |
|-------|-------|
| Dataset | `ds_source_superstore` |
| Field 1 | `itemName` — gets the file name |
| Field 2 | `size` — gets file size in bytes |
| Field 3 | `exists` — confirms file is present (true/false) |

### Copy Data — Source

| Field | Value |
|-------|-------|
| Source Dataset | `ds_source_superstore` |
| Column Delimiter | Comma (,) |
| Encoding | UTF-8 |

### Copy Data — Sink (Destination)

| Field | Value |
|-------|-------|
| Sink Dataset | `ds_destination_output` |
| Copy Behavior | PreserveHierarchy |
| Write Behavior | Append |

---

## Step 7 — Execute & Monitor Pipeline ⚠️

### Execution Methods

| Method | When to Use |
|--------|-------------|
| **Debug** | Immediate test run — no trigger needed |
| **Add Trigger → Now** | Manual one-time run after publishing |
| **Schedule Trigger** | Automated recurring runs |

### Expected Pipeline Results

| Metric | Expected Value |
|--------|---------------|
| Pipeline Status | Succeeded |
| Get Metadata Duration | < 5 seconds |
| Rows Read | 9,994 |
| Rows Written | 9,994 |
| Total Duration | < 2 minutes |
| Output | `superstore_output.csv` in `output-data` container |

### Schedule Trigger

| Field | Value |
|-------|-------|
| Name | `tr_daily_run` |
| Type | Schedule |
| Recurrence | Every 1 Day |
| Start | Today's date |

---

## Step 8 — Assign IAM Roles ⚠️

### Roles Used

| Role | What It Allows |
|------|---------------|
| **Reader** | View only — cannot create, modify, or delete |
| **Contributor** | Create/modify/delete — cannot assign roles |
| **Storage Blob Data Contributor** | Read + write blob data — required for ADF |

### Why Managed Identity?
ADF uses **Managed Identity** — Azure auto-creates a unique Azure AD identity for ADF. No stored passwords. This identity gets the `Storage Blob Data Contributor` role.

### Role Assignment 1: ADF → Storage

```
Storage Account → Access Control (IAM)
→ + Add → Add role assignment
→ Role: Storage Blob Data Contributor
→ Assign to: Managed identity → adf-celebal-2025
```

### Role Assignment 2: User → Reader

```
Storage Account → Access Control (IAM)
→ + Add → Add role assignment
→ Role: Reader
→ Assign to: User → your-email@domain.com
```

---

## Step 9 — End-to-End Pipeline with If Condition ⚠️

### Enhanced Architecture

```
Blob Source → Get Metadata → If Condition → Copy Data → Output
                                  |
                              True: Copy
                              False: Fail ("File not found or empty")
```

### If Condition Expression

```
@and(
    activity('Get Metadata1').output.exists,
    greater(activity('Get Metadata1').output.size, 0)
)
```

### Expression Breakdown

| Part | Meaning |
|------|---------|
| `.output.exists` | File physically exists in container — true/false |
| `.output.size` | File size in bytes from Get Metadata |
| `greater(..., 0)` | Size > 0 bytes — file is not empty |
| `@and(...)` | Both conditions must be true |
| True path | Execute Copy Data |
| False path | Execute Fail activity |

---

## ⚠️ Errors Encountered

### Error 1: Token Validation Failed

```
Token validation failed. A passthrough token was detected
without proper resource provider context.
```
**Cause**: Temporary Azure Portal session issue  
**Fix**: Page refresh (Ctrl+R) — resolved immediately

---

### Error 2: RequestDisallowedByAzure ← Main Blocker

```
Resource 'celebalstorage25abc' was disallowed by Azure.
Code: RequestDisallowedByAzure
Message: This policy maintains a set of best available regions
where your subscription can deploy resources.
```

**Cause**: Azure for Students has a Microsoft-managed policy restricting region availability  
**Regions tried**: Central India | East US | West US | East US 2 | West Europe | North Europe | Southeast Asia | UK South — all failed  
**Impact**: Storage Account creation blocked → ADF and Steps 2-9 not executed  
**Resolution**: Not resolvable on Azure for Students. A Pay-As-You-Go subscription does not have this restriction.

---

## 📚 Key Azure Concepts Learned

| Concept | Explanation |
|---------|-------------|
| Resource Group | Logical container grouping related Azure resources |
| Blob Storage | Object storage for unstructured files: CSV, images, logs |
| Azure Data Factory | Cloud ETL service — moves data between sources via visual pipelines |
| Linked Service | Connection string / auth definition for a data store |
| Dataset | Pointer to specific data: container, file, format, schema |
| Pipeline Activity | Single step: Get Metadata, Copy Data, If Condition, Fail |
| Integration Runtime | Compute engine ADF uses to run activities |
| Managed Identity | Auto-created Azure AD identity for services — no passwords |
| RBAC / IAM | Role-Based Access Control: Reader, Contributor, Storage Blob Data Contributor |
| Debug Mode | Immediate pipeline test without a trigger |
| Monitor Tab | Dashboard showing all pipeline runs with status and metrics |
| If Condition | Conditional branching — validates before running downstream steps |

---

## 📸 Screenshots Checklist

- [ ] Screenshot 1: Resource Group `rg-celebal-pipeline` overview page
- [ ] Screenshot 2: Storage Account with `input-data` and `output-data` containers
- [ ] Screenshot 3: `input-data` container showing `Sample - Superstore.csv` uploaded
- [ ] Screenshot 4: ADF Studio Author tab with navigation panel
- [ ] Screenshot 5: Linked Services page showing `ls_blob_storage` as Connected
- [ ] Screenshot 6: Datasets — both `ds_source_superstore` and `ds_destination_output`
- [ ] Screenshot 7: Pipeline canvas — Get Metadata connected to Copy Data
- [ ] Screenshot 8: IAM page — ADF with Storage Blob Data Contributor role
- [ ] Screenshot 9: Monitor tab — pipeline run with Succeeded status
- [ ] Screenshot 10: `output-data` container showing `superstore_output.csv`

---
