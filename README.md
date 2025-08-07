# US Wildfire Analytics
## High-level Overview of the Problem, Methods, and Outcomes
  
Over the last several years, wildfires have been increasingly destructive and unpredictable due to human activity and rising temperatures due to climate control. These factors have been responsible for the wildfires to grow in both frequency and intensity. To assist wildfire mitigation, this project developed three machine learning models to predict the total area burned, the average daily area burned, and the duration of the wildfires. These models were created using data from ERA5 which provided the necessary climate variables as well as data from the Global Fire Atlas which provided the wildfire behavior data. Five different models - Random Forest, Linear Regression, Decision Trees, K Nearest Neighbors, and XGBoost - were implemented and tested using 5-fold cross-validation and grid search. 
An interactive tableau dashboard was also created to visualize the wildfire locations, the predictive model outputs, and the airport infrastructure that is suitable for aerial firefighting. The dashboard includes distance-based analysis between the wildfires and the airports as well as airtanker bases. It also includes windspeed assessment to help evaluate aerial suppression efforts.  
 
## Objectives

- Predict the:
  - Total area burned
  - Daily average area burned
  - Duration of wildfires
- Analyze feasibility of aerial firefighting
- Visualize wildfire and climate insights through an interactive dashboard

## Deliverables

- Three optimized ML regression models:
  - Total area burned
  - Daily average area burned
  - Duration
- Interactive Tableau dashboard
- GitHub repository
- Final report and presentation
  
##  Data Sources:

- ERA5 Climate:
    - Source: Copernicus Climate Change Service
    - Format: Monthly NetCDF files
    - Content: ERA5 is a global reanalysis dataset that combines model outputs and historical observations to provide consistent daily estimates of near-surface atmospheric and land conditions. - - - Provides climate-based statistics such as surface temperature, wind, pressure, etc.
    - How it is used: Used as predictors for all the models
    - Link: https://cds.climate.copernicus.eu/datasets/derived-era5-single-levels-daily-statistics?tab=overview

- Global Fire Atlas:  
    - Source: NASA (via Zenodo)
    - Format: Shapefiles for each year from 2020 – 2024
    - Content: A shapefile of all wildfire burns in the 2020-2024 fire seasons with perimeter, fire area and perimeter information, start and end dates, as well as other metrics like average length of daily fire line, average daily fire growth, average daily speed, and other information 
    - How it is used: Provided the three target variables for the ML models to predict: Total area burned, daily average area burned, and duration of wildfires.
    - Link: https://zenodo.org/records/11400062  

- OurAirports collection
    - Source: OurAirports.com
    - Format: CSV files (airports.csv, runways.csv, countries.csv, regions.csv)
    - How it is used: Filtered for aerial firefighting suitability and joined with wildfire data for distance analysis and dashboarding requirements.
    - Link: https://ourairports.com/data/  
    - airports.csv
      Content: A comprehensive global list of public, private, and military airports, heliports, airtanker bases, and landing strips. It includes information such as location, identifiers (ICAO, IATA, local codes), latitude/longitude, elevation, and type.

    - runways.csv
      Content: Contains detailed information on runways associated with airports in the airports.csv file, including length, width, surface type, and runway identifiers. It is used to assess airport capabilities and suitability for various aircraft.

    - countries.csv
      Content: Provides a standardized list of world countries with corresponding codes.  It is used to provide country information for the other OurAirports datasets.

    - regions.csv
      Content: Lists administrative regions (e.g., states, provinces) by country, along with region codes. It helps provide regional information for the OurAirports datasets.

- WFIGS 2025 Fire Perimeters
  - Source: Wildland Fire Integrated Geospatial System
  - Format: CSV 
  - Content: Provides time, location, and spread of wildfires for 2025
  - How it is used: Used for the training models to simulate the production predictions
  - Link: https://data-nifc.opendata.arcgis.com/maps/nifc::wfigs-2025-interagency-fire-perimeters-to-date

- FAA NPIAS & Military Airport Lists
  - Source: FAA.gov and Military OneSource
  - Format: XLSX
  - Content: Identifier codes for any eligible firefighting airports
  - How it is used: Used to identify which airport bases can be used for firefighting suppression
  - Links: https://www.faa.gov/airports/planning_capacity/npias/current ; https://installations.militaryonesource.mil/

- Airtanker Base Directory
  - Source: National Wildfire Coordinating Group (NWCG)
  - Format: PDF
  - Content: Provides the official airtanker bases in the US
  - How it is used: Used to flag airtanker bases for firefighting capabilities that were part of NPIAS
  - Link: http://ruudleeuw.com/pdf/US%20Wildfire%20Air%20Tanker%20Base%20Directory%202019.pdf

- National Geographic Area Coordination Centers (GACC) Boundaries
  - Source: National Interagency Fire Center (NIFC)
  - Format: Shapefile
  - Content: Geographic boundaries of the US GACCs.
  - How it is used: Used to distinguish the wildfires and airports based on their GACC
  - Link: https://data-nifc.opendata.arcgis.com/datasets/nifc::national-gacc-boundaries-public/about

## Key methodologies:

After processing the 2020-2024 ERA5 and Global Fire Atlas, the  data was randomly split into 80% / 20% training and test sets.  The training set was used to perform 5-fold cross validation (CV) and grid search method to optimize and tune five ML models: random forest, XGBoost, linear regression, decision tree, and k nearest neighbor.  For each model, three models were trained for the three regression tasks to predict total area burned, average daily area burned, and duration.  Average 5-fold CV performance metrics were computed across the 5 folds for the models for each regression task.  Note, all models were supposed to have better average 5-fold CV performance metrics compared to a naive baseline that predicted the average training split target variable value.  Afterwards, the best model for each regression task was chosen based on their average 5-fold CV performance metrics.  Then, the best models were trained on the entire training set and tested against the test set.  The test set performance metrics were computed.  The best models were supposed to perform better than a test set baseline that predicted the average training split target variable value.  Afterwards, the best models were trained on both the training and test sets to make production models. To simulate production level predictions, they predicted total area burned, average daily area burned, and duration for WFIGS 2025 wildfires using their corresponding ERA5 predictors.  All findings were showcased in a dashboard.  

## Important Links:
- GitHub repo: https://github.com/MattVu123/us_wildfire.git
- OneDrive (where items that could not be pushed to GitHub are found): https://gmuedu-my.sharepoint.com/:f:/r/personal/vkaja_gmu_edu/Documents/Summer%202025%20Capstone/us_wildfire?csf=1&web=1&e=VWxM0S
- Dashboard: https://public.tableau.com/app/profile/azeem.holland/viz/wildfire_dashboard2/WildfireDashboard?publish=yes

## Overall structure, key content, setup, and tour

First, the GitHub repository should be cloned into a local repository.  Afterwards, there will be several folders inside the repository called config, dashboard, data, docs, notebooks, production_models, src, and trash.  Also, there should be several other files, such as a .gitignore, README.md, and requirements.txt.
The config folder should have a config.json file that contains files paths to important files in the repository.  

The dashboard folder contains dashboard files.  It should have files called Tableau Dashboard Link that contain the link to the Tableau public dashboard; wildfire_dashboard.twbx that is the Tableau Workbook file used to create the dashboard; and a file called ~wildfire_dashboard__22580.twbr that is a dashboard backup.  
The data folder should have subfolders called processed, production_model_predictions, and raw.  The data was too large to push to GitHub but can be found and downloaded from https://gmuedu-my.sharepoint.com/:f:/r/personal/vkaja_gmu_edu/Documents/Summer%202025%20Capstone/us_wildfire?csf=1&web=1&e=7EXiir.  

Inside the processed subfolder should be the processed data files called Wildfire_Weather_2020_2024_with_gacc.csv that is the combined training and test data that contains the weather predictors and wildfire behavior targets for wildfires from 2020-2024 that contains all variables (even the not needed ones) with their GACC with each row representing a wildfire; Wildfire_Weather_2020_2024.csv that is the same as Wildfire_Weather_2020_2024_with_gacc.csv but does not contain GACC information; training_wildfire_weather_2020_2024.csv that is just the 80% training data with only the weather predictors and wildfire behaviors targets needed for the ML tasks with each row representing a wildfire; test_wildfire_weather_2020_2024.csv that is the 20% test data with only the weather predictors and wildfire behavior targets with each row representing a wildfire; runways_processed.csv that contains the airport/airtanker base runway information where each row is a runway; nearest_airport_airtanker_bases_to_fires_final.csv where each row represents a wildfire along with the distance to its closest airport and airtanker base; full_wildfire_weather_2020_2024.csv that is the combined training and test sets with only the weather predictors and wildfire behaviors where each row represents a wildfire; BurnData that contains the wildfire behavior targets from the Global Fire Atlas where each row represents a wildfire; airports_runways_joined.csv that contains airport and runway information where each row represents an airport; and airports_processed.csv that contains airport information where each row represents an airport.

Inside the production_model_predictions subfolder should be the data files needed to simulate production level predictions.  The WildfireAtlas2020_2024_predictionvsactual_GACC.csv data file has each row representing a wildfire along with its weather predictors and wildfire behavior targets.  The actual and predicted target variables for each wildfire provided by the production models are provided.  The GACC of each wildfire is also provided.  WildFireAtlas2020_2024_predicitionvsactual.csv is the same as  WildfireAtlas2020_2024_predictionvsactual_GACC.csv but does not contain GACC information.  WFIGS2025_predictions_GACC.csv has each row representing a wildfire from the WFIGS 2025 dataset along with its weather predictors and wildfire behavior targets.  The actual and predicted target variables for each wildfire provided by the production models are provided.  The GACC of each wildfire is also provided.  The GACC_Size.csv file contains the GACC land area in acres, where it is used for visualizations.  

Inside the raw subfolder should be the raw data from the data sources.  The subfolder called WFIGS Incident Locations Perimeters Year to Date 2025 contains the WFIGS 2025 data.  The subfolder called shp contains the Global Fire Atlas data.  The our_airports_raw subfolder contains the OurAirports datasets.  The gacc_boundaries subfolder provides shapefiles and data for the GACC regions.  The era5_daily_2025 subfolder contains the 2025 ERA5 data used for WFIGS 2025 production predictions.  The era5_daily subfolder contains the 2020-2024 ERA5 data used for model training and testing for the Global Fire Atlas data.  Pms507-ATB-directory2018.pdf contains the list of all the airports that are also airtanker bases.  The npias.xlsx dataset contains all the airports that are part of NPIAS.  The military_airports.xlsx dataset contains all the airports that are military bases.  

Now, going back to the main repository, the docs folder should have any supplementary files.  The MLops and dashboarding workflow.png file should be a screenshot of the MLops and dashboading workflow.      

The notebooks folder contains all the developmental code used for the project.  It contains subfolders EDA, model_development, and processing_development.
The EDA subfolder has the following notebooks to develop and perform EDA.  The airports_eda.ipynb file performs EDA on the airport datasets.  The elevation_vs_runway_length.twbx file is the tableau workbook used to create the elevation vs. runway length graph.  The interactive_airport_map.html file is the interactive map of airports and airtanker bases.  The Predictor Variables EDA.ipynb file is the EDA for the weather features.  

The model_development subfolder has the following notebooks used to develop and experiment with the models.  Compute_baseline.ipynb computes the 5-fold CV and test set baselines.  The create_model_and_2025_data.ipynb contains the code to create the production models and make production predictions.  The decision tree.ipynb file contains the code to experiment with the decision tree model.  The knn_model.ipynb file contains the code to experiment with the KNN model.  The linear_regression.ipynb file contains the code to experiment with the linear regression model.  The random_forest_xgboost.ipynb file contains the code to experiment with the random forest and xgboost models.  

The processing_development subfolder has the following notebooks used to develop the data processing code.  Airport_data_processing.ipynb contains the code to process the OurAirports dataset collection.  The burn_data_table_creation.ipynb contains the code to process the Global Fire Atlas data to obtain wildfire behaviors.  Create_working_dataset.ipynb contains the code to combine the ERA5 climate features with the Global Fire Atlas wildfire behavior targets.  Data_processing_playground.ipynb contains code to play around with the data in the project.  Era5_daily_api_request.ipynb contains code to send an API request to obtain the ERA5 dataset.  nearest_airport_airtanker_bases_to_fires_final.ipynb contains the code to compute the distance between a wildfire and its closest airport and airtanker base.  Specify_gacc.ipynb contains code to specify the GACC that wildfire and airport/airtanker base is located in.  

The production_models folder should contain the production models and parameters.  These were unable to be uploaded to GitHub but could be found here: https://gmuedu-my.sharepoint.com/:f:/r/personal/vkaja_gmu_edu/Documents/Summer%202025%20Capstone/us_wildfire?csf=1&web=1&e=edDDj4.  The folder should have a subfolder called era5_daily_downloads that contains the era5 data from 2020-2025.  The spreadparams.pkl file is the parameters of the production random forest model to predict average daily area burned.  The sizeparams.pkl file is the parameters of the production random forest model to predict total area burned.  The size_model.pkl file is the production random forest model to predict total area burned.  The fire_spread_model.pkl file is the production random forest model to predict average daily area burned.  The durationparams.pkl file is the parameters of the production random forest model to predict duration.  The duration_model.pkl file is the production random forest model to predict duration.

The src folder contains code consisting of modularized functions of the production modeling and data processing code.  The purpose of this is to provide re-useable functions to create the production modeling and processing code.  Inside the src folder there should be subfolders modeling_modules and processing_modules.  

The modeling_modules subfolder contains module code to produce the production and best models.  create_model_and_2025_data.py contains functions to create production models and make production predictions.  random_forest_xgboost.py contains functions to train and perform analytics on the random forest, which was the best model.

The process_modules subfolder contains module code to process the data.  Airport_data_processing.py contains functions to process the OurAirports data.    burn_data_table_creation.py contains functions to process the global fire atlas to create the burn data.  Create_working_dataset.py contains functions to create the training and test data.  era5_daily_api_request.py contains the functions to API request the ERA5 datasets.  nearest_airport_airtanker_bases_to_fire_final.py contains functions to compute the distance between wildfires and airports/airtanker bases.  specificy_gacc.py contains functions to specify the GACC region of wildfires and airports/airtanker bases.  

Now, going back to the main repository, the trash folder just contains trash code that may be useful for later.  The .gitignore contains a list of files to ignore.  The README.md contains useful information about the repository.  The requirements.txt contains library and module requirements. 

So, a quick rundown on how to use this is to clone the repository; run the code in the notebooks folder where the data gets processed first and then modeling occurs and EDA occurs; and run the code in the production_models folder. 








