# 19. Developer Environment Setup Guide

### Prerequisites
- **Operating System:** Windows 10/11, macOS, or Ubuntu Linux
- **Docker & Docker Compose:** Docker Desktop with 8GB+ RAM allocated
- **Python:** 3.10, 3.11, or 3.12 (with `venv`)
- **Java:** JDK 11, 17, or 21 (`JAVA_HOME` configured)

### Step-by-Step Installation
```powershell
# 1. Clone repository and navigate to folder
cd job-market-intelligence

# 2. Create and activate Python virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 3. Install Python dependencies
pip install -r requirements.txt
pip install reportlab

# 4. Start Docker Infrastructure
docker-compose -f docker/docker-compose.yml up -d
```
