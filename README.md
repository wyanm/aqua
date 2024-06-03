# AQUA

The aim of AQUA is to provide historic AIS-information to spot Russian oil tankers that are circumventing the sanctions - also known as Shadow Fleet.

This github page shares the code and some of the aggregated data-files depending on the maximum allowed size of git-lfs. The AIS-data is collected ad converted to hourly data, which then is further prepared to joined data (between ship position data and static ship informations) and grouped data (per ship data). Anomaly detection is also attempted. 

A dashboard can be found here (currently under a free server tier so it could stop prematurely):

A manually curated list with the current findings ob ships that should be more closely looked at is here:    

A support from your side could help me finance a server to run the dashboard there.

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/V7V5YWBHN)


### Structure

01_Get_Raw_Data
02_Aggregate_Hourly_Data
03_Aggregate_Ship_Data
04_Join_Prepare
05_Group_By_Ship
06_Anomaly_Detection
07_Dashboard 

### Todo

- navigation and communication modes adding to grouping
- Adding to grouping also the ETAs
- sog_std issue in 02 (can it be added later?)
- github
- data to one folder; change scripts and docker images to fit everything


Question:
- do navigation status say correctly if in harbor? additionally spot if a ship remains at a point for longer time?

### weird vessels
#### BLACK SEA
Valpiave, 256812000, after 19.05.24, to NVS and then to Santa Panagia / Italy
Yanbu, 636023504, after 17.05.24, to NVS and then to kalamata / Greece
Belgorod, 273259530, after 22.05.24 at black sea to NVS
Sankt Peterburg, 273443330, after 21.05.24 at black sea to kavkaz
Minerva Joy, 229446000, after 19.05.24 at black sea and then to Said/Egypt
Seatribute, 215544000, after 17.05.24 at nvs and then to Tranmere/Grand Britain
NS Stella, 626401000, after 26.05.24 to black sea
Prometheus Light, 538008103, after 30.05.24 to nvs
Odysseus, 626316000, wrong destination malta on 20.05.24 and then to nvs
Kapitan Pshenitsin, 273344600,25.05.24 going to ru tmr
Delta Maria, 241356000, 17.05.24 to nvs and then to Trieste/Italy
Juvenis, 511101307, 28.05.24, going to ru tua
Safeen Elona, 538010608, after 29.05.24 in black sea to nvs (before to Fos/france)
East 1, 477594700, after 26.05.24 to nvs after being to mersin
Minerva Gloria, 215307000, 28.05.24, to nvs after itmlz and grpir
Argolis, 215504000, 27.05.24, rutua to trdrc
Seavelvet, 215514000, after 24.05.24 to nvs after itmlz and grpir
Seavigour, 249349000, after 25.05.24 to nvs after Trieste/Italy and grpir
Hana, 626320000, after 21.05.24 to nvs after Beirut
Chrysopigi, 249971000, after 23.05.24 black sea for orders
Lachin, 423488100, after 19.05.24 to ru tmr
Mando one, 518998659, after 24.05.24 to black sea for orders
Vladimir Tikhonov, 626431000, after 20.05.24 to nvs and "black sea for orders"
Armada leader, 273448000, after 24.05.24 to ru tmr
Sredina, 357263000, after 28.05.24 to nvs from lyzaw previously
Hera, 574005330, after 29.05.24 to nvs (previously only saying turkey as dest)
Seginus, 621819086, after 30.05.24, black sea for orders
Nargis, 626283000, after26.05.24 to nvs previously in Greece
NS Bora, 626376000, after 28.05.24 to nvs from inprt
Twerk, 636013411, around 31.05.24 spotted near Egypt ru tua to eg psd
Kriti King, 636020097, after 31.05.24 to nvs from Genova/italy and Malta previously
Paschalis DD, 636022535, after 31.05.24 to nvs previously in Greece
Tatiana, 636022736, after 24.05.24 to nvs previously unknown
Bronx, 636022962, after 28.05.24 to nvs previously in malta
Kaluga, 273298630, after 28.05.24 to kavkaz previouly in mersin
Breeze, 572242220, after 28.05.24 to taman russia
Cherry Tonda, 352003888, after 23.05.24 in black sea
Minerva Grace, 352003888, after22.05.24 really in Bulgaria? Now to Barcelona/Spain



Chem Helen (636013337), from 27.05.24 Murmansk to Libya (false claim first saying to skagen/Denmark for orders)
Chiba (352002692) 21.05.24 from Primorsk to Turkey (false claim first saying to Skaw/Denmark for orders)
Ocean AMZ (518998877) 19.05.24 from Primorsk to Morocco
kusto (518999185), from 17.05.24 Ust-Luga to Primorsk (while falsly claiming Malta to Tallinn trip) than on 20.05.24 to Said/Egypt

weird not yet communicated:
Yildiz(271050993), 26.05.24 from Ust-Luga to Mersin/Turkey
Arabela (518999128) 19.05.24 from Ust-Luga to Mundra/India, 
Ursus Arctos (636022405) 18.05.24 from Ust-Luga to Said/Egypt
Sakarya (352002168) 20.05.24 from Murmansk to Kyaukpyu/Myanmar
Esentepe (538010161)  27.05.24 from Murmansk to MTOPL (Maledives?) for orders
Zouzou N (636014251) unknown date of departure from Murmansk to Said/Egypt
Legend (574005290) 28.05.24 from Ust-Luga claiming to drive towards Suez hiding end-destination
BLUE (304868000) 17.05.24 from Murmansk to Said/Egypt
Clearocean Apollon (636019153) 18.05.24 from Ust-Luga to Said/Egypt
Sivas (352002488) 24.05.24 coming back from Said/Egypt first to Ust_luga then going to Primark
Grace Ferrum (636023538) 21.05.24 leaving from Ust-Luga to Paranagua/Brazil
Sealeo (256721000) 19.05.24 leaving from Ust_luga to  Capetown/South Africa
Shusha (423519100) 25.05.24 leaving from Ust-Luga to Aliaga/Turkey
Thorin (518999161) claiming as destination "gulf of finland", and when leaving on 25.05.24 from Primorsk first EGsuez and then Said/Egypt
Palermo, 538008065, after 17.05.25 from RU LED to Merina/Turkey


soon leaving russian port:
Minerva M (352001530) in Ust-Luga
Valeriy Zelenko (273333950) in Ust-Luga
Sagitta (352001616) going from Ust-Luga to Primarsk

Other observations:
Boris Vilkitsky (212654000) for orders hiding its way to Zeebrugge losing cargo there

old weird not communicated 212770000 -> Cannot find anymore?


### Version Overview

#### V0.0.3 - 03.06.2024-03.06.2024 - Improved structure 
#### V0.0.2 - 25.05.2024-02.06.2024 - dockerized with auto-restart - data should now be more precise and without gaps anymores
#### V0.0.1 - 12.05.2024-24.05.2024 - First version - Correct Position Data starts 17.05 as on previous days the data was not OK yet.
