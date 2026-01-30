"""
================================================================================
CI/CD SETUP GUIDE - Databricks Pipeline with GitHub Actions
================================================================================

This guide explains how to set up GitHub Actions to automatically trigger your
Databricks pipeline on code changes.

Pipeline Name: Complete-MLOps-Pipeline
Pipeline ID: f94b89a8-8688-4a03-b285-e88727b7f403

================================================================================
STEP 1: PREPARE YOUR GITHUB REPOSITORY
================================================================================

1. Initialize a Git repository in your pipeline directory:
   
   cd "/Workspace/Users/mbayebabacar.gueye@bennen.tech/Complete-MLOps-Pipeline-folder"
   git init
   git add .
   git commit -m "Initial commit: Complete MLOps Pipeline"

2. Create a GitHub repository and push your code:
   
   git remote add origin https://github.com/bentechno/complete-mlops-pipeline.git
   git branch -M main
   git push -u origin main

================================================================================
STEP 2: CREATE DATABRICKS PERSONAL ACCESS TOKEN
================================================================================

1. Go to your Databricks workspace
2. Click on your user profile (top right) → Settings
3. Go to "Developer" → "Access tokens"
4. Click "Generate new token"
5. Give it a name (e.g., "GitHub Actions CI/CD")
6. Set expiration (recommended: 90 days)
7. Click "Generate"
8. **COPY THE TOKEN IMMEDIATELY** (you won't see it again)

================================================================================
STEP 3: CONFIGURE GITHUB SECRETS
================================================================================

1. Go to your GitHub repository
2. Click "Settings" → "Secrets and variables" → "Actions"
3. Click "New repository secret"
4. Add the following secrets:

   Secret Name: DATABRICKS_HOST
   Value: https://your-workspace.cloud.databricks.com
   
   Secret Name: DATABRICKS_TOKEN
   Value: <paste your token from Step 2>
   
   Secret Name: PIPELINE_ID
   Value: f94b89a8-8688-4a03-b285-e88727b7f403

================================================================================
STEP 4: CREATE GITHUB ACTIONS WORKFLOW
================================================================================

Create the following file in your repository:
.github/workflows/databricks-pipeline.yml

---
name: Complete MLOps Pipeline CI/CD

on:
  push:
    branches:
      - main
      - develop
    paths:
      - 'transformations/**'
      - 'cicd/**'
  pull_request:
    branches:
      - main
  workflow_dispatch:
    inputs:
      full_refresh:
        description: 'Run full refresh'
        required: false
        type: boolean
        default: false

jobs:
  validate-pipeline:
    name: Validate Pipeline Syntax
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install pyspark requests
      
      - name: Validate Python syntax
        run: |
          python -m py_compile transformations/**/*.py

  trigger-pipeline:
    name: Trigger Complete MLOps Pipeline
    runs-on: ubuntu-latest
    needs: validate-pipeline
    if: github.event_name == 'push' || github.event_name == 'workflow_dispatch'
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install requests
      
      - name: Trigger Pipeline Update
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          PIPELINE_ID: ${{ secrets.PIPELINE_ID }}
        run: |
          python cicd/trigger_pipeline.py --action start-and-wait ${{ github.event.inputs.full_refresh && '--full-refresh' || '' }}
      
      - name: Pipeline Status
        if: always()
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          PIPELINE_ID: ${{ secrets.PIPELINE_ID }}
        run: |
          python cicd/trigger_pipeline.py --action status

  dry-run-on-pr:
    name: Dry Run on Pull Request
    runs-on: ubuntu-latest
    needs: validate-pipeline
    if: github.event_name == 'pull_request'
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Comment on PR
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '✅ Complete MLOps Pipeline validation passed! Ready for merge.'
            })
---

================================================================================
STEP 5: ALTERNATIVE - USING DATABRICKS CLI
================================================================================

You can also use the official Databricks CLI in GitHub Actions:

---
name: Complete MLOps Pipeline with CLI

on:
  push:
    branches: [main]

jobs:
  deploy-and-run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      
      - name: Configure Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks configure --token <<EOF
          $DATABRICKS_HOST
          $DATABRICKS_TOKEN
          EOF
      
      - name: Start Pipeline Update
        run: |
          databricks pipelines start-update ${{ secrets.PIPELINE_ID }}
---

================================================================================
STEP 6: TESTING YOUR SETUP
================================================================================

1. Manual Trigger (Recommended for first test):
   - Go to GitHub → Actions tab
   - Select "Complete MLOps Pipeline CI/CD" workflow
   - Click "Run workflow"
   - Choose branch and options
   - Click "Run workflow"

2. Automatic Trigger:
   - Make a change to any file in transformations/
   - Commit and push to main branch
   - GitHub Actions will automatically trigger

3. Monitor the pipeline:
   - Check GitHub Actions logs
   - Check Databricks pipeline UI for "Complete-MLOps-Pipeline"

================================================================================
STEP 7: ADVANCED CONFIGURATIONS
================================================================================

Environment-Specific Deployments:
- Use different secrets for dev/staging/prod
- Create separate workflows for each environment
- Use GitHub Environments for approval gates

Notifications:
- Add Slack notifications on success/failure
- Send email alerts
- Create GitHub Issues on failures

Scheduled Runs:
- Add cron schedule to workflow:
  on:
    schedule:
      - cron: '0 2 * * *'  # Run daily at 2 AM UTC

================================================================================
TROUBLESHOOTING
================================================================================

Issue: "Authentication failed"
Solution: Verify DATABRICKS_TOKEN is correct and not expired

Issue: "Pipeline not found"
Solution: Verify PIPELINE_ID matches your pipeline (f94b89a8-8688-4a03-b285-e88727b7f403)

Issue: "Permission denied"
Solution: Ensure token has pipeline execution permissions

Issue: "Timeout waiting for pipeline"
Solution: Increase timeout in trigger_pipeline.py

================================================================================
USEFUL COMMANDS
================================================================================

# Test trigger script locally
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
python cicd/trigger_pipeline.py --action start

# Check pipeline status
python cicd/trigger_pipeline.py --action status

# Stop running pipeline
python cicd/trigger_pipeline.py --action stop

# Full refresh
python cicd/trigger_pipeline.py --action start --full-refresh

================================================================================
NEXT STEPS
================================================================================

1. Set up branch protection rules
2. Require PR reviews before merging
3. Add automated testing
4. Set up monitoring and alerting
5. Document your deployment process

================================================================================
"""

# This file serves as documentation only
pass
