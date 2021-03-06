# Clean Subscribers Data

unit = "district"
metric = "mean_distance"
for(unit in c("district", "ward")){
  for(metric in c("mean_distance", "stdev_distance")){
    
    print(paste(unit, metric,  "---------------------------------------------"))
    
    # Set parameters -------------------------------------------------------------
    if(unit %in% "district"){
      RAW_DATA_PATH <- file.path(PANELINDICATORS_PATH)
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
      
      #### Load Data
      df_day <- read.csv(file.path(RAW_DATA_PATH, 
                                   "clean",
                                   "i7_2.csv"), 
                         stringsAsFactors=F) 
    }
    
    if(unit %in% "ward"){
      RAW_DATA_PATH <- file.path(PANELINDICATORS_PATH)
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
      
      #### Load Data
      df_day <- read.csv(file.path(RAW_DATA_PATH, 
                                   "clean",
                                   "i7_3.csv"), 
                         stringsAsFactors=F) 
    }
    
  
    # For wards, remove if tower is down ---------------------------------------
    if(unit %in% "ward"){
      
      towers_down <- read.csv(file.path(PROOF_CONCEPT_PATH, 
                                        "outputs", 
                                        "data-checks", 
                                        "days_wards_with_low_hours_I1_panel.csv"))
      
      towers_down <- towers_down %>%
        dplyr::select(region, date) %>%
        mutate(tower_down = T) %>%
        mutate(date = date %>% as.character)
      
      df_day <- df_day %>%
        left_join(towers_down, 
                  by = c("date" = "date",
                         "home_region" = "region"))
      
      df_day[[metric]][df_day$tower_down %in% TRUE] <- NA
    }
    
    # Daily ----------------------------------------------------------------------
    print("day")
    
    df_day_clean <- df_day %>% 
      
      tp_standardize_vars("date", "home_region", metric) %>%
      
      # Clean datset
      tp_clean_date() %>%
      tp_fill_regions(admin_sp) %>%
      tp_complete_date_region() %>%
      tp_add_polygon_data(admin_sp) %>%
      
      # Interpolate/Clean Values
      tp_interpolate_outliers(NAs_as_zero = T, outlier_replace="both") %>%
      tp_replace_zeros(NAs_as_zero = T) %>%
      tp_less15_NA(threshold = 0) %>%
      
      # Percent change
      tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, paste0("i7_",metric,"_daily_base.csv"))) %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "day", OD = F) %>%
      tp_add_label_baseline(timeunit = "day", OD = F)
    
    ## Export
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH, paste0("i7_daily_",metric,".Rds")))
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, paste0("i7_daily_",metric,".csv")), row.names=F)
    
    
    # Weekly ---------------------------------------------------------------------
    print("week")
    
    df_week_clean <- df_day_clean %>% 
      
      dplyr::select(date, region, value) %>%
      
      tp_standardize_vars("date", "region", "value") %>%
      
      # Clean datset
      tp_clean_week() %>%
      tp_agg_day_to_week(fun="mean") %>%
      tp_fill_regions(admin_sp) %>%
      tp_complete_date_region() %>%
      tp_add_polygon_data(admin_sp) %>%
      
      # Interpolate/Clean Values
      #tp_interpolate_outliers(NAs_as_zero = T) %>%
      #tp_replace_zeros(NAs_as_zero = T) %>%
      #tp_less15_NA() %>%
      
      # Percent change
      tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, paste0("i7_",metric,"_weekly_base.csv")),
                                 type = "weekly") %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "week", OD = F) %>%
      tp_add_label_baseline(timeunit = "week", OD = F) 
    
    
    ## Export
    saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH,
                                     paste0("i7_weekly_",metric,".Rds")))
    write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
                                       paste0("i7_weekly_",metric,".csv")), 
              row.names=F)
    
    
  }
}

