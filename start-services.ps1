# Powershell script to boot up all PayFlow microservices in separate windows

$mvnCmd = "C:\Users\Admin\Desktop\payflow\maven\apache-maven-3.9.6\bin\mvn.cmd"

Write-Host "Booting Eureka Server (Port 8761)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd eureka-server; & '$mvnCmd' spring-boot:run"

# Wait 10 seconds for Eureka to start up fully
Start-Sleep -Seconds 10

Write-Host "Booting API Gateway (Port 8080)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd api-gateway; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Auth Service (Port 8081)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd auth-service; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Account Service (Port 8082)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd account-service; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Transaction Service (Port 8083)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd transaction-service; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Fraud Service (Port 8084)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd fraud-service; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Notification Service (Port 8085)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd notification-service; & '$mvnCmd' spring-boot:run"

Write-Host "Booting Ledger Service (Port 8086)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd ledger-service; & '$mvnCmd' spring-boot:run"

Write-Host "All 8 microservices are booting up in separate terminals!" -ForegroundColor Green
Write-Host "You can access the Eureka dashboard at http://localhost:8761" -ForegroundColor Yellow
