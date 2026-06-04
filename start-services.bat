@echo off
set MVN_CMD="C:\Users\Admin\Desktop\payflow\maven\apache-maven-3.9.6\bin\mvn.cmd"

echo Booting Eureka Server (Port 8761)...
start "Eureka Server" cmd /k "cd eureka-server && %MVN_CMD% spring-boot:run"

echo Waiting 10 seconds for Eureka to start...
timeout /t 10 /nobreak > nul

echo Booting API Gateway (Port 8080)...
start "API Gateway" cmd /k "cd api-gateway && %MVN_CMD% spring-boot:run"

echo Booting Auth Service (Port 8081)...
start "Auth Service" cmd /k "cd auth-service && %MVN_CMD% spring-boot:run"

echo Booting Account Service (Port 8082)...
start "Account Service" cmd /k "cd account-service && %MVN_CMD% spring-boot:run"

echo Booting Transaction Service (Port 8083)...
start "Transaction Service" cmd /k "cd transaction-service && %MVN_CMD% spring-boot:run"

echo Booting Fraud Service (Port 8084)...
start "Fraud Service" cmd /k "cd fraud-service && %MVN_CMD% spring-boot:run"

echo Booting Notification Service (Port 8085)...
start "Notification Service" cmd /k "cd notification-service && %MVN_CMD% spring-boot:run"

echo Booting Ledger Service (Port 8086)...
start "Ledger Service" cmd /k "cd ledger-service && %MVN_CMD% spring-boot:run"

echo All 8 microservices are booting up in separate terminals!
echo You can access the Eureka dashboard at http://localhost:8761
pause
