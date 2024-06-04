# AQUA

The aim of AQUA is to spot Russian oil tankers that are circumventing the sanctions - the Shadow Fleet. It provides historic AIS-data, a dashboard, and some analyses. 

You can find in the folder "ships_of_interest" csv-files of tankers leaving russian black sea and north-western ports and with additional manually made observations. In the folder "data" you can find all the collected data in parquet-files. You can use the dashboard to visualize and browse the available data.

This project is made on a best effort. You can also support it in order to be able to finance a server for the dashboard and data-collection:
[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/V7V5YWBHN)


### Structure of the Scripts

All of the scripts in this repo are under the folder "src":

- 01_Get_Raw_Data: Collects the AIS-Data in real-time
- 02_Aggregate_Hourly_Data: The ship positioning data is aggregated per hour so that less data is required to store
- 03_Aggregate_Ship_Data: The same is done for static ship data
- 04_Join_Prepare: Both datasets (ship positioning data and static ship data) are combined/joined together on an hourly basis.
- 05_Group_By_Ship: Data is converted on a per-ship level (time-dimension gets away and data gets aggregated).
- 06_Anomaly_Detection: Some anomaly detection is being calculated to spot unusual cases - however this is not implemented at the moment.
- 07_Dashboard: The dashboard to visualize the routes and analyze the ships. Makes it possible to manually label data and make comments.



### Version Overview

#### V0.0.3 - 03.06.2024-04.06.2024 - Improved structure for publication
#### V0.0.2 - 25.05.2024-02.06.2024 - dockerized with auto-restart - data should now be more precise and without gaps anymores
#### V0.0.1 - 12.05.2024-24.05.2024 - First version - Correct Position Data starts 17.05 as on previous days the data was not OK yet.
