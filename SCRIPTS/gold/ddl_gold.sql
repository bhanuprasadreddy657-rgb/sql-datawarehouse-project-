/**
================================================================================
DDL script:create gold views 
================================================================================
script purpose:
         this script creates the views for gold year in data ware house.
         the gold layer represents the dimention and fact tables (star schema)

         each view performs transformations combines data from silver layer 
         to producea clean and structured,enriched and business ready dataset.
USAGE:
      this views can be queried directly for analytics and reporting.
/**
================================================================================
CREATE DIMENSION:gold.dim_customers 
================================================================================
