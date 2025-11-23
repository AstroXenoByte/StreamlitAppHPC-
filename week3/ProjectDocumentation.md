Weekly mentor meetings are conducted to validate progress, gather feedback, and ensure alignment with project objectives.
2.2 Tools and Techniques
Tool	Purpose
Python 3.8+	Primary programming language
Pandas & NumPy	Data manipulation and analysis
CSV Files	Data storage format
Streamlit	Dashboard framework
Plotly	Data visualization
Git & GitHub	Version control
pytest	Testing framework
Nginx	Web server for Streamlit
________________________________________
3.0 PROJECT MANAGEMENT PLANi
3.1 Tasks
3.1.1 Week 1: PBS Logs Understanding & Environment Setup
Description: Establish understanding of PBS log structures and set up complete development environment.
Deliverables:
•	PBS log format documentation
•	Configured development environment
•	GitHub repository structure
•	Sample PBS log files
•	Week 1 mentor meeting & validation
Resources: HPC documentation, development hardware, software tools
Constraints: Limited access to production HPC systems, must complete within Week 1
Risks: Cannot access real PBS logs (use synthetic data), log format variations, team unfamiliarity with HPC concepts
________________________________________
3.1.2 Week 2: Build ETL Pipeline (Extract, Transform, Load)
Description: Develop core data pipeline that extracts data from PBS logs, transforms it into structured format, and saves it as CSV files.
Deliverables:
•	ETL pipeline script (etl_pipeline.py)
•	Output CSV files with processed data
•	Data schema documentation
•	Pipeline documentation
•	Week 2 mentor meeting & validation
Resources: Python libraries (pandas, numpy), CSV file handling
Constraints: Full log file sizes could not be uploaded on github, CSV files must be efficiently structured
Risks: Log format inconsistencies, performance bottlenecks with large files, CSV file size limitations
________________________________________
3.1.3 Week 3: HPC Usage Metrics & Streamlit Dashboard
Description: Develop analytics layer that calculates HPC cluster usage metrics from CSV data and create Streamlit dashboard to visualize them. Configure Nginx to serve the Streamlit application.
Deliverables:
•	Metrics calculation module 
•	Streamlit dashboard application (streamlit_processed.py)
•	Dashboard views for metrics visualization
•	Nginx configuration for serving Streamlit
•	Metrics specification document
•	Week 3 mentor meeting & validation
Resources: Python libraries (pandas, numpy, scipy), Streamlit, Plotly, Nginx web server
Constraints: Metrics must calculate within 5 seconds, dashboard must load quickly
Risks: Slow metric calculations, dashboard performance issues, Nginx configuration problems
________________________________________
3.1.4 Week 4: System Monitoring & Health Dashboard
Description: Create bash scripts to collect live VM, CPU, memory, and network statistics. Build Streamlit health and logs pages to display real-time system stats and monitor application logs.
Deliverables:
•	Bash script for collecting system stats (vm_stats.sh)
•	Streamlit health dashboard section
•	Streamlit logs monitoring section
•	Stats visualization
•	System health monitoring documentation
•	Week 4 mentor meeting & validation
Resources: Bash scripting, Linux system commands (top, vmstat, iostat), Streamlit, Plotly
Constraints: Must collect stats without impacting system performance, real-time updates required
Risks: Performance overhead from stats collection, data refresh rate issues, log file size management
________________________________________
3.1.5 Week 5: VM Cluster Deployment on Sebowa Cloud & Poster Design
Description: Deploy the PBS Logs Analysis System on a VM cluster using Sebowa Cloud infrastructure and create a professional project poster for presentation.
Deliverables:
•	VM cluster configured on Sebowa Cloud
•	System deployed and running on cloud infrastructure
•	Project poster design (academic/conference format)
•	Deployment documentation
•	Week 5 mentor meeting presentation & final validation
Resources: Sebowa Cloud account, VM instances, Canva (poster design)
Constraints: Cloud resource limits, poster must follow academic presentation standards
Risks: Cloud service availability issues, VM configuration problems, insufficient cloud resources
________________________________________
3.2 Timetable
Week	Focus Area	Key Milestone
Week 1	Foundation & Setup	Environment ready, PBS logs understood
Week 2	ETL Pipeline	Data flowing from logs to CSV files
Week 3	Metrics & Dashboard	Metrics calculated and displayed in Streamlit, Nginx configured
Week 4	System Monitoring	Bash scripts collecting stats, health & logs dashboard live
Week 5	Deployment & Poster	System on Sebowa Cloud, poster completed
________________________________________
4.0 CONCLUSION
This Project Management Plan outlines a structured 5-week approach to developing the PBS Logs Analysis System. The iterative model ensures each component is developed, tested, and validated before moving to the next phase. 
Weekly deliverables provide clear milestones and allow for early detection of issues. With proper risk management and resource allocation, the project is positioned for successful completion and deployment.
