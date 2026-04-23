#  PSI Malaria Analytics – End-to-End Microsoft Fabric Pipeline

![Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue)
![Power BI](https://img.shields.io/badge/PowerBI-Analytics-yellow)
![Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Delta](https://img.shields.io/badge/Delta-Lakehouse-green)
![Status](https://img.shields.io/badge/Project-Proof-of-Concept-brightgreen)

---

##  Overview
This project is a **PSI-aligned malaria program performance analytics solution** demonstrating a full **end-to-end cloud data engineering and BI workflow** for public health decision-making.

It transforms raw epidemiological, economic, and program data into **actionable KPIs and country prioritization tiers** across **18 Sub-Saharan African countries**.

---

##  Business Objective

- Identify high malaria burden regions  
- Evaluate health system strength  
- Measure PSI intervention effectiveness  
- Enable data-driven prioritization  

### Priority Classification
- 🔴 Priority 1 – Immediate Action  
- 🟠 Priority 2 – Accelerate  
- 🟡 Priority 3 – Monitor  
- 🟢 On Track  

---

##  Architecture

### Medallion Architecture
```
Bronze → Silver → Gold → Semantic Model → Power BI
```

| Layer | Description |
|------|------------|
| Bronze | Raw API ingestion |
| Silver | Cleaned and validated data |
| Gold | KPI modeling and star schema |
| Semantic Model | Business logic and DAX |
| Power BI | Visualization |

---

##  Data Pipeline

### Data Sources
- WHO GHO  
- WHO Africa Observatory  
- World Bank  
- Synthetic PSI dataset  

### Key Features
- API-based ingestion  
- Incremental processing (RunId)  
- Schema enforcement  
- Data quality validation  

---

##  KPI Framework (Z-Score Normalization)

### Core KPIs

| KPI | Description |
|-----|------------|
| MalariaRiskScore | Incidence + deaths |
| HealthSystemScore | GDP + expenditure |
| PSIProgramScore | Efficiency + cost |
| OverallPSIScore | Weighted composite |

```
OverallPSIScore =
    (-0.5 * MalariaRisk_Z)
    + (0.3 * HealthSystem_Z)
    + (0.2 * PSIProgram_Z)
```

---

##  Semantic Model

- DirectLake enabled  
- Star schema  
- Calculation groups  
- Optimized DAX measures  

---

##  Power BI Dashboard

### Executive Overview
- KPI cards  
- Trend analysis  
- Risk ranking  
- Priority tiers  

---

##  Screenshots

### 🔹 Workspace / Architecture
<!-- Replace below with your uploaded image -->
<img width="1360" height="642" alt="PSI Workspace" src="https://github.com/user-attachments/assets/d740ba31-4ce8-4284-af19-4fb8c5352005" />


### 🔹 Data Pipeline
<img width="1350" height="637" alt="Pipeline Activity" src="https://github.com/user-attachments/assets/f3ef925e-a88e-4f2d-91ae-d0cc29062d7c" />


### 🔹 Dashboard Overview
<img width="1358" height="638" alt="Executive Overview" src="https://github.com/user-attachments/assets/2a81e954-7d8c-4fa9-afe3-88fd293a0c9a" />


### 🔹 Semantic Model
<img width="1355" height="634" alt="Semantic Model" src="https://github.com/user-attachments/assets/6d2367d9-e026-4c7e-926d-af903d6dd8d5" />


---

##  Engineering Best Practices

- Incremental pipelines  
- Modular notebooks  
- Delta Lake storage  
- Merge-based upserts  
- Schema evolution handling  

---

##  Data Quality

- Automated checks  
- Null validation  
- Audit logging  

---

##  CI/CD

- GitHub integration  
- Version control  
- Reproducible pipelines  
 

---

##  Notes

- PSI data is **synthetic**  
- Real data sourced from WHO & World Bank  
- Model calibrated for realistic scoring  

---

##  Key Outcomes

- End-to-end Fabric solution  
- Scalable data pipeline  
- Decision-ready analytics  

---

##  Future Enhancements

- Real-time streaming  
- ML predictions  
- Geospatial analytics  


