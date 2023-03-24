# Partition-Based CDC POC

This is a proof-of-concept project to explore the feasibility of building a Change Data Capture (CDC) solution at the partition level for an Oracle database. The goal of the project is to determine whether it is possible to efficiently and reliably capture changes to individual partitions of large tables, and propagate those changes to a Hadoop file system.

## Project Description

The project uses Apache Airflow to trigger a daily job that connects to the Oracle database and queries the ```ALL_TAB_PARTITIONS``` view to retrieve a list of partitions that have been modified since the last run. It then uses an Oracle Data Pump tool to extract the modified partitions from the database and export them to a file. Finally, the file is copied to the Hadoop file system and the modified partitions are replaced in place.

The project includes sample code to demonstrate the process, as well as configuration files for Apache Airflow and the Oracle Data Pump tool. The goal is to test the scalability and efficiency of the approach, as well as identify any potential challenges or limitations.

## Getting Started
To get started with the project, you will need:

* An Oracle database with sample data
* A Hadoop file system
* Apache Airflow installed and configured
* The Oracle Data Pump tool installed and configured
* Once you have these components set up, you can clone the repository and run the sample code to test the process.

## Contributing
This project is intended as a proof-of-concept and is not actively maintained. However, contributions are welcome and will be considered on a case-by-case basis.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
This project was inspired by the need for a more efficient and scalable approach to capturing changes to large Oracle databases. Thanks to the Apache Airflow and Hadoop communities for their contributions to the project.
