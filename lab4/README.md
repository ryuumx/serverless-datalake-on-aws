# Part 4: Visualize Data with Quicksight

## 1. Setting Up QuickSight

In this step we will visualize your datasets using QuickSight

Login to Amazon Quick Sight Console & complete the registration & sign-up

* GoTo: [Quicksight Console](https://us-east-1.quicksight.aws.amazon.com/sn/start)
* You may be asked to Sign Up for Quicksight. Proceed to Create your QuickSight account with an Enterprise plan.
* Create your QuickSight account:
    * Edition: **Use Role Based Federation(SSO)**
    * QuickSight region: **US East (N. Virginia)**
    * QuickSight account name: **YOUR_USERNAME**
    * Check the following:
        * Enable autodiscover of data and users in your Amazon Redshift.
        * Amazon Athena
        * Amazon S3: Select **YOUR_USERNAME-datalake-demo-bucket** > **Select buckets**
        * **Go to Amazon Quicksight**
		
Note: if you have errors creating a Quicksight account, it is possible that the username has already been in-use. Try again with a unique username

Welcome to the QuickSight console!

![QuickSight console](./img/qs-console.png)


## 2. Adding a New Dataset

* GoTo: https://us-east-1.quicksight.aws.amazon.com/sn/start
* On top right, Click - **Manage Data**
    * Click - **New Data Set**
    * Click - **Athena** 
    * New Athena data source
        * Data source name: **awslabs_db**
    * Choose your table:
        * Database: contain sets of tables: select - **awslabs_db**
        * Tables: contain the data you can visualize : select - **processed**
        * Click - **Select**
    * Finish data set creation:
        * Select - **Import to SPICE for quicker analytic**
	* SPICE is Amazon QuickSight's in-memory optimized calculation engine, designed specifically for fast, ad hoc data visualization
        * Click **Visualize**


## 3. Using Amazon Quick Sight to Visualize Our Processed Data

* Visualization 1: Heat map of users and tracks they are listening to

    In this step, we will create a visualization that show us which users are listening to repetitive tracks.

    * On the bottom-left panel - **Visual types**
        * Hover on icon there to see names of the visualizations
        * Click on - **Heat Map**
    * On top-left panel - **Fields list**
        * Click -  **device_id**
        * Click - **track_name**
    * Just above the visualization you should see **Field wells**: [**Rows - device_id**] [**Columns - track_name**]

    ![Heatmap](./img/qs-heatmap.png)

    If you hover on dark blue patches on the heatmap you will see that those particular users are listening to same track repeatedly.

2. Visualization 2: Tree map of most played Artist Names

    In this step we will create a visualization that shows who are the most played artists

    * On top-left - Click on '**+ Add**' > **Add Visual**, this will add a new panel to the right pane
    * On the bottom-left panel - **Visual types**
        * Hover on icon there to see names of the visualizations
        * Click on - **Tree Map**
    * On top-left panel - **Fields list**
        * Click - **artist_name**

    ![Treemap](./img/qs-treemap.png)

    Play around and explore Amazon QuickSight Console. Try out filters, other visualization types, etc.


## 4. Using ML Insights for Forecasting and Anomaly Detection

In this step, we will explore generating narratives for data sets using built-in ML capabilities in Quicksight.
ML Insights have [minimum requirements](https://docs.aws.amazon.com/quicksight/latest/user/ml-data-set-requirements.html) for data sets based on granularity and data size.

For this lab, we will use a sample dataset that looks like this. 

|Date|Product Categories|Geo|Revenue|
|:-:|:--:|:--:|:--:|
|1/1/17|Digital|Turkey|1738.04848|
|1/1/17|Movies|Turkey|3359.74848|

* Prepare the data set

    * Download the sample data .csv [here](./qs-sample.csv)
    * Add a New Data Set here: [Manage Data sets](https://us-east-1.quicksight.aws.amazon.com/sn/data-sets)
	* Click on New data set
	* Upload a file
	![upload-a-file](./img/upload-a-file.png)
	* Review the file contents and click on Next > Visualize data
	![sample-details](./img/sample-details.png)
    * Build a simple time series Visual for the data set.
	* Click on the Line Chart visual at the bottom panel Visual types
	* Click on **Date**, **Revenue** and **Product Categories** in that order.
    * Wait for the data to get loaded.
	
    ![timeseries-sample](./img/timeseries-sample.png)
	
* Explore ML-powered Insights

    * Click on the **Insights** tab on the left navigation.
    * Observe the **Suggested insights** on the side panel. You may see suggested insights for:
	* Top 2 performing Product Categories for _School Supplies (+12.27%)_ and _Home Services (+0.6%)_
	* Total Revenue compounded growth rate of _0.06%_ over 30 days
	* Significant Day-Over-Day Revenue decrease on Nov 17
	![Insights](./img/insights.png)
	
    * Add the Suggested Insights to your Sheet by clicking on :heavy_plus_sign:

    * Wait for the data to get populated.

## 5. Filtering Data using Filters

Let's filter for a single timeseries chart. 

* Click on Filter in the side navigation.
* Select the :heavy_plus_sign: and **Product Categories**
    * Select Filter type: **Filter list**
    * Search and select **School Supplies**
    * Click **Apply**
	![Single TS](./img/single-ts.png)
	
* In the visual, select the v-shaped dropdown, and select **Hide "other" categories**

    ![Hide others](./img/hide-others.png)

* Explore other options for filtering data, such as *Custom filters for String matching*, *OR operators* and *Datetime range filters*
	

## 6. Custom Insights using Autonarratives

Autonarrative is a natural-language summary for text descriptions to simplify understanding a visual. [See a list of Autonarrative options](https://docs.aws.amazon.com/quicksight/latest/user/auto-narratives.html)

* On the Growth rate insight, click on the option (...) icon and **Customize narrative**
* Build your own insights using Computations, Parameters and Functions.

![config-autonarrative](./img/configure-narrative.png)


## 7. Exploring Anomalies 

* In the Insights tab again, click on **Add anomaly to sheet**
    * In Configure anomaly detection, under **Combinations to be analyzed**, select **All**
    * Select **Run now**.
* The result of the Anomaly Detection suggests a _lower than expected Revenue for Sept 27 2018_

    ![anomaly-result](./img/anomaly-result.png)

* Click on option (...) icon and **Explore Anomalies** to filter Anomaly analysis by Severity, Direction and more. 

## 8. Forecasting

* Change the timeseries visual into a timeseries aggregated by Revenue. Click on the visual and remove the **Color: Product Categories** dimension.
* In the visual, click on the option (...) icon, and select **Add forecast**
* Observe the generated Revenue forecast including values for *Expected*, *Upper bound* and *Lower bound*.

    ![Forecast](./img/forecast.png)
	
* Edit the forecast for parameters on *Periods forward*, *Periods backward*, *Interval* and *Seasonality* to fine-tune the predicted values.


## Exploring additional QuickSight features

Checkout the [Quicksight Youtube Channel](https://www.youtube.com/channel/UCqtI0cKSreCwUUuKOlA1tow) for more! :arrow_forward:
