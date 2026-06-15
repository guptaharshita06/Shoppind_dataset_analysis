# Azure End-to-End Data Pipeline
**Celebal Technologies Internship | Week Assignment**

---

## What I Built

I built a data pipeline on Microsoft Azure that reads a CSV file from Blob Storage and copies it to a destination container using Azure Data Factory. I set up all the required resources, linked services, datasets, and the pipeline with Get Metadata and Copy Data activities.

**Dataset used:** Superstore Sales Dataset from Kaggle  
**Tech used:** Azure Storage Account, Azure Data Factory, IAM, Blob Containers

---

## Step 1 — Created a Resource Group

First I created a Resource Group to keep all my Azure resources organized in one place.

**Configuration:**
- Subscription: Azure for Students
- Resource group name: `rg-celebal-pipeline`
- Region: East US

I went to `portal.azure.com`, searched for Resource Groups, clicked **+ Create**, filled in the name and region, then hit **Review + Create**. It got created successfully.

> A Resource Group is basically a folder that holds all your Azure resources together so you can manage billing and delete everything at once if needed.

---

## Step 2 — Created Storage Account and Blob Containers

I created a Storage Account to store my CSV file.

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

Both containers were created with **Private** access level.

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
| Monitor (chart icon) | Where I checked pipeline run status |
| Manage (wrench icon) | Where I set up linked services and integration runtimes |
| Home | Dashboard with recent activity |

---

## Step 5 — Created Linked Service and Datasets

### Linked Service

I connected ADF to my Storage Account through a Linked Service.

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

I dragged **Get Metadata** from the General activities section onto the canvas. In the Settings tab I configured:
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

After setting up both activities I clicked **Publish All**.

---

## Step 7 — Ran the Pipeline (Error Encountered)

I clicked **Debug** to run the pipeline.

The **Get Metadata** activity ran successfully and returned the file info. However, the **Copy Data** activity failed — the data did not get copied to the `output-data` container. Since the Copy Data activity failed, the pipeline run did not complete successfully and I was not able to publish the final output.

**What happened:**
- Get Metadata → ✅ Succeeded
- Copy Data → ❌ Failed
- Pipeline output → not published

**Possible reasons for the Copy Data failure:**
- ADF did not have the required permissions to write to the `output-data` container (IAM role not yet assigned at this point)
- The sink dataset configuration may have had an issue
- The destination container may not have been accessible to ADF without the Storage Blob Data Contributor role

> This error made me realize that IAM roles need to be set up before running the pipeline, not after. ADF needs the **Storage Blob Data Contributor** role on the Storage Account to be able to read from source and write to destination.

---

## Steps 8 & 9 — IAM Roles and If Condition (Planned)

Even though I ran into the error in Step 7, I understood what needed to be done for the remaining steps.

### IAM Roles (Step 8)

To fix the Copy Data error, I would need to:

1. Go to Storage Account → **Access Control (IAM)**
2. Click **+ Add → Add role assignment**
3. Assign **Storage Blob Data Contributor** to ADF's Managed Identity (`adf-celebal-2025`)
4. Also assign **Reader** role to my own Azure account

| Role | Assigned To | Purpose |
|------|-------------|---------|
| Storage Blob Data Contributor | `adf-celebal-2025` (Managed Identity) | Allows ADF to read and write blob data |
| Reader | My Azure account | View-only access to the storage resource |

### End-to-End Pipeline with If Condition (Step 9)

The advanced version of the pipeline would add an **If Condition** activity between Get Metadata and Copy Data to validate the file before copying:

```
Blob Source → Get Metadata → If Condition → Copy Data → Output
                                  |
                              True: Copy
                              False: Fail ("File not found or empty")
```

**Expression:**
```
@and(
    activity('Get Metadata1').output.exists,
    greater(activity('Get Metadata1').output.size, 0)
)
```

This runs Copy Data only if the file exists AND has content (size > 0 bytes).

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

---

## Key Concepts I Learned

**Resource Group** — keeps all related Azure resources organized in one place

**Blob Storage** — stores unstructured files like CSVs on the cloud. Containers are like folders inside it.

**Azure Data Factory** — cloud ETL tool that lets you build data pipelines visually

**Linked Service** — how ADF connects to a data source, works like a connection string

**Dataset** — tells ADF what file to read or write: which container, file name, format

**Get Metadata** — ADF activity that fetches info about a file (name, size, exists) before doing anything with it

**Copy Data** — ADF activity that moves data from source to destination

**If Condition** — adds conditional logic to pipeline: run Copy only if file is valid

**Managed Identity** — secure way for ADF to access Storage without storing any passwords

**IAM / RBAC** — controls who can do what in Azure. ADF needs Storage Blob Data Contributor role to access blob data

**Monitor tab** — where you check if pipeline ran successfully and how long each activity took

---

## What I Would Do Differently

If I were to redo this, I would assign the IAM roles (Storage Blob Data Contributor to ADF's Managed Identity) **before** running the pipeline for the first time. The Copy Data activity failed most likely because ADF did not have write permission on the `output-data` container at that point.

---

*Celebal Technologies Internship 2025*
---
