# ACO Scripts

Utilities for processing OCR output from **YAI Global**, preparing HOCR files, and generating PDFs through the task queue used by the ACO workflow.

Repository: https://github.com/rrasch/aco-scripts

> ⚠️ **Note:** The README inside the repository directory may be outdated or in flux. Use the instructions below.

---

# Overview

These scripts assist with the following tasks:

* Generating PDfs used to upload to vendor
* Renaming OCR files delivered by vendor
* Preparing HOCR files for searchable PDF generation
* Adding task-queue jobs for book processing

Most scripts support a `--dry-run` option to preview commands before executing them.

---

# Prerequisites

The scripts assume the following environment:

* Access to the ACO content directory
* Access to the YAI Global transfer directory
* The `add-mb-job` command available in the environment
* Access to the task queue (RabbitMQ)

Typical directory structure:

```
/content/prod/rstar/content/aub/aco/
    wip/
        se/
            <book_id>/

/content/prod/rstar/xfer/dropbox/yaiglobal/
    outbox/
        <book_id>/
    processing/
        <book_id>/
```

---

# Installation

Clone the repository:

```bash
git clone https://github.com/rrasch/aco-scripts.git
cd aco-scripts
```

---

# Pre-Processing Workflow

A typical workflow for pre-processing a book looks like this:

1. Generate low resolution PDFs used an input to extract OCR

Each step is described below.

---

# 1. Generate PDFs use to extract OCR

To generate PDFs before uploading to YAI Global, add a `book_publisher:make_yaiglobal_upload_pdf` job.

Use the `-e` (extra arguments) option with `-f` to force removal of existing files in the `aux` directory.

Example:

```bash
add-mb-job -m <rabbitmq.host> -s book_publisher:make_yaiglobal_upload_pdf \
    -r /content/prod/rstar/content/aub/aco \
    -e "-f" aub_aco001518
```

or use the wrapper script:

```bash
./add_book_jobs.py --make-yaiglobal-upload-pdf aub_aco001518
```

---

# Post-Processing Workflow

A typical workflow for post-processing a book looks like this:

1. Rename OCR files from YAI Global
2. Copy `.hocr` files into the book's `aux` directory
3. Add a task queue job to generate PDFs

Each step is described below.

---

# 1. Rename YAI Global OCR Files

If you are completing all steps of the workflow, the only manual step may be renaming the OCR files provided by YAI Global.

Run:

```bash
/usr/local/dlib/aco-scripts/rename_yaiglobal_ocr.py <dmaker-dir> <yaiglobal_dir>
```

Example:

```bash
/usr/local/dlib/aco-scripts/rename_yaiglobal_ocr.py \
    /content/prod/rstar/content/aub/aco/wip/se/aub_aco001518/data \
    /content/prod/rstar/xfer/dropbox/yaiglobal/processing/batch0000_bis/aub_aco001518
```

Use `--dry-run` to preview the actions before executing them.

---

# 2. Copy HOCR Files

Before generating PDFs, copy the `.hocr` files into the book's `aux` directory.

The scripts assume that HOCR files exist in this location before the PDF generation job is submitted.

---

# 3. Generate PDFs from HOCR

Add a task queue job to generate PDFs from the HOCR files.

This script acts as a wrapper around:

```
add-mb-job -s book_publisher:hocr2pdf
```

Run:

```bash
/usr/local/dlib/aco-scripts/add_book_jobs.py --hocr2pdf <book_id>
```

Example:

```bash
/usr/local/dlib/aco-scripts/add_book_jobs.py --hocr2pdf aub_aco001518
```

---

# Generalized Job Script

The job creation script has been generalized and renamed.

Run:

```bash
./add_book_jobs.py [--dry-run] --hocr2pdf <book_id> [<book_id> ...]
```

Example:

```bash
./add_book_jobs.py --dry-run --hocr2pdf aub_aco001518
```

Always run with `--dry-run` first to confirm that the correct commands will be executed.

---

# Dry Run Mode

Most scripts support the following option:

```
--dry-run
```

This prints the commands that would be executed without performing any actions.

Using `--dry-run` is strongly recommended before running scripts in production.

---

# Example End-to-End Workflow

```bash
# rename OCR files from YAI Global
rename_yaiglobal_ocr.py \
    /content/prod/rstar/content/aub/aco/wip/se/aub_aco001518/data \
    /content/prod/rstar/xfer/dropbox/yaiglobal/processing/batch0000_bis/aub_aco001518

# copy hocr files to aux directory
cp *.hocr /content/prod/rstar/content/aub/aco/wip/se/aub_aco001518/aux/

# add PDF generation job
add_book_jobs.py --hocr2pdf aub_aco001518
```
