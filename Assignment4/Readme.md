# Azure End-to-End Data Pipeline
**Celebal Technologies Internship | Week Assignment**

---

## What I Built

I built a complete data pipeline on Microsoft Azure that reads a CSV file from Blob Storage, validates it using Get Metadata, and copies it to a destination container using Azure Data Factory. I also set up IAM roles and added an If Condition activity for metadata validation.

**Dataset used:** Superstore Sales Dataset from Kaggle  
**Tech used:** Azure Storage Account, Azure Data Factory, IAM, Blob Containers

---

## Step 1 — Created a Resource Group

First thing I did was create a Resource Group to keep all my Azure resources organized in one place.

**What I used:**
- Subscription: Azure for Students
- Resource group name: `rg-celebal-pipeline`
- Region: East US

I went to `portal.azure.com`, searched for Resource Groups, clicked **+ Create**, filled in the name and region, then hit **Review + Create**. It got created successfully.

> A Resource Group is basically a folder that holds all your Azure resources together so you can manage them, monitor billing, and delete everything at once if needed.

---

## Step 2 — Created Storage Account and Blob Containers

Next I created a Storage Account to store my CSV file.

**Storage Account config:**
- Name: `celebalstorage25abc`
- Resource group: `rg-celebal-pipeline`
- Region: East US
- Performance: Standard
- Redundancy: LRS (Locally Redundant Storage)
- Primary service: Azure Blob Storage

After the storage account was created, I created two containers inside it:

| Container | Purpose |
|-----------|---------|
| `input-data` | To store the source CSV file |
| `output-data` | To receive the output after pipeline runs |

I created both containers with **Private** access level.

---

## Step 3 — Uploaded the Superstore CSV

I downloaded the Superstore dataset from Kaggle and uploaded it to the `input-data` container.

**File details:**
- File name: `Sample - Superstore.csv`
- Size: ~1 MB
- Rows: 9,994
- Columns: 21 (Order ID, Ship Mode, Segment, Category, Sales, Profit, etc.)

To upload: opened `input-data` container → clicked **Upload** → selected the CSV → done. The file showed up in the container with its name and size.

---

## Step 4 — Created Azure Data Factory

I created an ADF instance to build and run the data pipeline.

**ADF config:**
- Name: `adf-celebal-2025`
- Resource group: `rg-celebal-pipeline`
- Region: East US
- Version: V2
- Git: configured later

After deployment I clicked **Launch Studio** which opened the ADF authoring UI in a new tab.

**The ADF Studio has 4 main sections:**

| Section | What it does |
|---------|-------------|
| Author (pencil icon) | Where I built pipelines, datasets, linked services |
| Monitor (chart icon) | Where I checked if pipeline runs succeeded or failed |
| Manage (wrench icon) | Where I set up linked services and integration runtimes |
| Home | Dashboard with recent activity |

---

## Step 5 — Created Linked Service and Datasets

### Linked Service

Before building the pipeline I needed to connect ADF to my Storage Account. I did this through a Linked Service.

- Went to **Manage → Linked services → + New**
- Selected **Azure Blob Storage**
- Name: `ls_blob_storage`
- Authentication: Account Key (auto-filled from my subscription)
- Selected `celebalstorage25abc` from the dropdown
- Clicked **Test connection** → got **Connection successful**
- Clicked **Create**

### Source Dataset

- Name: `ds_source_superstore`
- Linked service: `ls_blob_storage`
- Format: DelimitedText (CSV)
- Container: `input-data`
- File: `Sample - Superstore.csv`
- First row as header: Yes
- Import schema: From connection/store

### Destination Dataset

- Name: `ds_destination_output`
- Linked service: `ls_blob_storage`
- Format: DelimitedText (CSV)
- Container: `output-data`
- File: `superstore_output.csv`
- First row as header: Yes

After creating both datasets I clicked **Publish All** to save.

---

## Step 6 — Built the Pipeline

I created a pipeline named `pl_superstore_copy` with two activities.

### Pipeline flow

```
input-data (Blob)  →  Get Metadata  →  Copy Data  →  output-data (Blob)
```

### Get Metadata Activity

I dragged **Get Metadata** from the General activities section onto the canvas. Then in the Settings tab:
- Dataset: `ds_source_superstore`
- Added 3 field list items:
  - `itemName` — to get the file name
  - `size` — to get file size in bytes
  - `exists` — to check if file is present

### Copy Data Activity

I dragged **Copy Data** from Move & Transform onto the canvas and connected it to Get Metadata using the green arrow.

**Source tab:**
- Dataset: `ds_source_superstore`
- Column delimiter: Comma
- Encoding: UTF-8

**Sink tab:**
- Dataset: `ds_destination_output`
- Copy behavior: PreserveHierarchy

After setting everything up I clicked **Publish All**.

---

## Step 7 — Ran and Monitored the Pipeline

I clicked **Debug** to run the pipeline immediately without a trigger.

The Output panel at the bottom showed both activities going from **In Progress** to **Succeeded**.

**Results from Copy Data details (clicked the glasses icon):**

| Metric | Value |
|--------|-------|
| Rows read | 9,994 |
| Rows written | 9,994 |
| Data read | ~1 MB |
| Status | Succeeded |
| Duration | Under 2 minutes |

I then went to the **Monitor** tab and verified the pipeline run was listed as **Succeeded**.

I also verified the output — went to Storage Account → `output-data` container → `superstore_output.csv` was present.

### Schedule Trigger

I also created a schedule trigger:
- Name: `tr_daily_run`
- Type: Schedule
- Recurrence: Every 1 day

---

## Step 8 — Assigned IAM Roles

### Enabled Managed Identity on ADF

I went to my ADF resource → **Settings → Managed identities** and confirmed System assigned status was **On**. This gives ADF its own identity in Azure AD without needing passwords.

### Role Assignment 1 — ADF gets Storage access

- Went to Storage Account → **Access Control (IAM)**
- Clicked **+ Add → Add role assignment**
- Role: **Storage Blob Data Contributor**
- Assigned to: Managed identity → selected `adf-celebal-2025`
- Clicked Review + assign

### Role Assignment 2 — Reader for my account

- Same IAM page → **+ Add → Add role assignment**
- Role: **Reader**
- Assigned to: my own Azure email
- Clicked Review + assign

**Difference between the two roles:**

| Role | What it allows |
|------|---------------|
| Reader | View only — cannot create or modify anything |
| Contributor | Create, modify, delete — cannot assign roles |
| Storage Blob Data Contributor | Read and write blob data specifically |

---

## Step 9 — End-to-End Pipeline with If Condition

I updated the pipeline to add an **If Condition** activity between Get Metadata and Copy Data. This checks that the file actually exists and has data before copying.

### Updated pipeline flow

```
Blob Source  →  Get Metadata  →  If Condition  →  Copy Data  →  Output
                                      |
                                  True: Copy
                                  False: Fail ("File not found or empty")
```

### If Condition expression I used

```
@and(
    activity('Get Metadata1').output.exists,
    greater(activity('Get Metadata1').output.size, 0)
)
```

This means: run Copy Data only if the file exists **AND** its size is greater than 0 bytes. If either condition fails, the pipeline goes to the False path and throws a Fail activity with the message: *"File not found or empty"*.

I ran this updated pipeline with Debug and all 3 activities showed **Succeeded** in the Monitor tab.

---

## Pipeline Architecture Summary

```
Azure Blob Storage (input-data)
        |
        ▼
  Azure Data Factory
        |
   [Get Metadata] ──── checks: exists? size > 0?
        |
   [If Condition] ──── True path continues
        |
   [Copy Data] ──────── reads CSV from input-data
        |
        ▼
Azure Blob Storage (output-data)
   superstore_output.csv ✓
```

---

## Resources Created

| Resource | Name |
|----------|------|
| Resource Group | `rg-celebal-pipeline` |
| Storage Account | `celebalstorage25abc` |
| Source Container | `input-data` |
| Destination Container | `output-data` |
| Azure Data Factory | `adf-celebal-2025` |
| Linked Service | `ls_blob_storage` |
| Source Dataset | `ds_source_superstore` |
| Destination Dataset | `ds_destination_output` |
| Pipeline | `pl_superstore_copy` |
| Schedule Trigger | `tr_daily_run` |
| IAM Role 1 | ADF → Storage Blob Data Contributor |
| IAM Role 2 | My account → Reader |

---

## Key Concepts I Learned

**Resource Group** — keeps all related Azure resources organized in one place

**Blob Storage** — stores unstructured files like CSVs on the cloud. Containers are like folders inside it.

**Azure Data Factory** — cloud ETL tool that lets you build data pipelines visually without writing code

**Linked Service** — how ADF connects to a data source. Works like a connection string.

**Dataset** — tells ADF what file to read or write: which container, which file, what format

**Get Metadata** — ADF activity that fetches info about a file (name, size, exists) before doing anything with it

**Copy Data** — ADF activity that actually moves data from source to destination

**If Condition** — adds logic to pipeline: run Copy only if file is valid

**Managed Identity** — secure way for ADF to access Storage without storing any passwords

**IAM / RBAC** — controls who can do what in Azure. Reader = view only, Contributor = full access, Storage Blob Data Contributor = blob-specific access

**Monitor tab** — where you check if your pipeline ran successfully and how long each activity took

---
