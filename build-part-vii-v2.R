

########################
######################## USE THESE FUNCTIONS
########################

build_rdb_table <- function (url, table.name, v.map) 
{
    doc <- NULL
    try(doc <- xml2::read_xml(url), silent = T)
    if (is.null(doc)) {
        return(NULL)
    }
    xml2::xml_ns_strip(doc)
    OBJECTID <- get_object_id(url)
    URL <- url
    EIN <- xml2::xml_text(xml2::xml_find_all(doc, "//Return/ReturnHeader/Filer/EIN"))
    V_990NAMEpost2014 <- "//Return/ReturnHeader/Filer/BusinessName/BusinessNameLine1Txt"
    V_990NAME_2013 <- "//Return/ReturnHeader/Filer/BusinessName/BusinessNameLine1"
    V_990NAMEpre2013 <- "//Return/ReturnHeader/Filer/Name/BusinessNameLine1"
    name.xpath <- paste(V_990NAME_2013, V_990NAMEpre2013, V_990NAMEpost2014, 
        sep = "|")
    NAME <- xml2::xml_text(xml2::xml_find_all(doc, name.xpath))
    V_990TFpost2013 <- "//Return/ReturnHeader/ReturnTypeCd"
    V_990TFpre2013 <- "//Return/ReturnHeader/ReturnType"
    tax.form.xpath <- paste(V_990TFpost2013, V_990TFpre2013, 
        sep = "|")
    FORMTYPE <- xml2::xml_text(xml2::xml_find_all(doc, tax.form.xpath))
    V_990FYRpost2013 <- "//Return/ReturnHeader/TaxYr"
    V_990FYRpre2013 <- "//Return/ReturnHeader/TaxYear"
    fiscal.year.xpath <- paste(V_990FYRpost2013, V_990FYRpre2013, 
        sep = "|")
    TAXYR <- xml2::xml_text(xml2::xml_find_all(doc, fiscal.year.xpath))
    group.names <- find_group_names(table.name = table.name)
    df <- get_table(doc, group.names, table.name)
    if (is.null(df)) {
        return(NULL)
    }
    # v.map <- get_var_map(table.name = table.name)
    df <- re_name(df, v.map)
    rdb.table <- data.frame(OBJECT_ID = OBJECTID, EIN = EIN, 
        NAME = NAME, TAXYR = TAXYR, FORMTYPE = FORMTYPE, URL = URL, 
        df, stringsAsFactors = F)
    return(rdb.table)
}


build_tables <- function( index=NULL, years=NULL, table.name, log.name="BUILD-LOG.txt" )
{
  
  start.build.time <- Sys.time()
  
  session.info <- sessionInfo()
  dump( list="session.info", file="SESSION-INFO.R" )
  
  zz <- file( log.name, open = "wt" )
  sink( zz, split=T )
  sink( zz, type = "message", append=TRUE )
  
  print( paste0( "There are ", nrow(index), " returns in this build." ) )
  print( paste0( "Years: ", paste0( years, collapse=";" ) ) )
  print( paste0( "You have ", parallel::detectCores(), " cores available for parallel processing." ) )
  print( paste0( "DATABASE BUILD START TIME: ", Sys.time() ) )
  print( paste0( "###########################" ) )
  print( paste0( "###########################" ) )
  
  
  for( i in years )
  {
    
    print( paste0( "STARTING LOOP ", i ) )

    # index.i <- dplyr::filter( index, TaxYear == i )
    # print( paste0( "There are ", nrow(index.i), " returns in ", i, "." ) )
  
    # groups <- split_index( index.i, group.size = 200 )
    # print( paste0( "There are ", length(groups), " groups being sent for parallel collection." ) )
  
    # dir.create( as.character(i) )
    # setwd( as.character(i) )
    
    start.time <- Sys.time()
    failed.urls <- build_year( i, table.name )
    end.time <- Sys.time()
    # setwd( ".." )
  
    print( paste0( "There were ", length(failed.urls), " failed URLS" ) )
    print( paste0( "Time for the ", i, " loop: ", round( difftime( end.time, start.time, units="hours" ), 2 ), " hours" ) )
    print( paste0( "###########################" ) )
    print( paste0( "###########################" ) )
    
    # saveRDS( failed.urls, paste0("FAILED-URLS-", i, ".rds") )
  
  }

  for( i in years )
  { bind_data( year=i ) }
  
  end.build.time <- Sys.time()
  print( paste0( "DATABASE BUILD FINISH TIME: ", Sys.time() ) )
  print( paste0( "TOTAL BUILD TIME: ", round( difftime( end.build.time, start.build.time, units="hours" ), 2 ), " HOURS" ) )

  sink( type="message" )
  sink()      # close sink
  close(zz)   # close connection
  file.show( log.name )
        
  t <- Sys.time()
  t <- gsub( " |:", "-", t )
  savehistory( "build-history.Rhistory" ) 
  
  return( NULL )
}





split_index <- function( index, group.size=1000 )
{
  urls <- index$URL
  f <- ( ( 1 : length(urls) ) + group.size - 1 ) %/% group.size
  f <- paste0( "g", f )
  f <- factor( f, levels=unique(f) )
  url.list <- split( urls, f )
  return( url.list )
}






build_year <- function( year, table.name )
{

  dir.create( year )
  setwd( year )

  total.start.time <- Sys.time()

  index.sub <- dplyr::filter( index, TaxYear == year )
  split.index <- split_index( index.sub )

  results.list <- list()
  failed.urls <- NULL


  for( i in 1:length(split.index) )
  {


    urls <- split.index[[i]]
    
    start.time <- Sys.time()

    for( j in 1:length(urls) )
    {
      url <- urls[j]

      try( temp.table <- build_rdb_table( url, table.name, v.map ) )

      results.list[[j]] <- temp.table 

      if( is.null(temp.table) )
      {
         failed.urls[[ length(failed.urls) + 1 ]] <- url
      }


    }


    end.time <- Sys.time()

    b.num <- substr( 10000 + i, 2, 5 ) 
      
    print( paste0( "Batch ", b.num ) )
    print( end.time - start.time )
    # print( paste( "There are ", length(failed.urls), " failed XML URLs to re-try." ) )
  
    ## add random string so filenames not duplicated
    ## when using parallelization 
    # time <- format(Sys.time(), "%b-%d-%Y-%Hh-%Mm")
    # rand <- paste( sample(LETTERS,5), collapse="" )
    # time <- paste0( "time-", time, "-", rand  )

    df <- dplyr::bind_rows( results.list )
    saveRDS( df, paste0( "batch", "-", b.num, ".rds" ) )
    write.csv( df, paste0( "batch", "-", b.num, ".csv" ), row.names=F )
  }


  total.end.time <- Sys.time()

  failed.urls <- unlist( failed.urls )
  dump( list="failed.urls", file="FAILED-URLS.R" )  

  setwd( ".." )

  print( paste0( "YEAR ", year, " COMPLETE" ) )
  print( "TOTAL RUN TIME:" )
  print( difftime( total.start.time, total.end.time, units="hours") )

  return( failed.urls )

}





bind_data <- function( year )
{
   
  setwd( year ) 
  dir.create( "COMPILED" ) 
    
  file.names <- dir()
  these <- grepl( "*.rds", file.names )
  
  loop.list <- file.names[ these ]

  d <- NULL

  for( i in loop.list )
  {
     d.i <- readRDS( i )
     d <- dplyr::bind_rows( d, d.i )
  }

  nrow( d )
  d <- unique(d)
  nrow( d )

  # setwd( "../COMPILED" )
 
   
  write.csv( d, paste0( "COMPILED/PARTVII-", year, ".csv", row.names=F ) )
  saveRDS(   d, paste0( "COMPILED/PARTVII-", year, ".rds" )  )    

  setwd( ".." )       
}




#########################################
#########################################   F9-P07-T01-COMPENSATION
#########################################

library( dplyr )
library( irs990efile )
library( jsonlite )


setwd( "E:/part-vii-v5" )

# https://www.dropbox.com/s/pdfls0e1jz9trvi/index-nccs.rds?dl=0

index <- readRDS( "index-nccs.rds" )
index <- dplyr::filter( index, FormType %in% c("990","990EZ") )


find_group_names( table.name="F9-P07-T01-COMPENSATION" )

# map to standardize variable names across table versions 
v.map <- get_var_map( table.name="F9-P07-T01-COMPENSATION" )
x1 <- c("F9_07_COMP_DTK_COMP_OTH","F9_07_COMP_DTK_COMP_OTH")
x2 <- c("ExpenseAccountOtherAllwncAmt","ExpenseAccountOtherAllowances")
d.add <- data.frame( VARIABLE=x1, XSD_VARNAME=x2 )
v.map <- bind_rows( v.map, d.add )


table.name <- "F9-P07-T01-COMPENSATION"




### 2009-2011

years <- c( 2009:2011 ) %>% as.character()

build_tables( 
    index=index, 
    years=years, 
    table.name=table.name,
    log.name="BUILD-LOG-09-11.txt")





### 2012-2013

years <- c( 2012:2013 ) %>% as.character()

build_tables( 
    index=index, 
    years=years, 
    table.name=table.name,
    log.name="BUILD-LOG-12-13.txt")
    
    
    
    
### 2014-2015    
    
years <- c( 2014:2015 ) %>% as.character()

build_tables( 
    index=index, 
    years=years, 
    table.name=table.name,
    log.name="BUILD-LOG-14-15.txt")





### 2016-2017    
    
years <- c( 2016:2017 ) %>% as.character()

build_tables( 
    index=index, 
    years=years, 
    table.name=table.name,
    log.name="BUILD-LOG-16-17.txt")
    
    
    
    
### 2018-2020    
    
years <- c( 2018:2020 ) %>% as.character()

build_tables( 
    index=index, 
    years=years, 
    table.name=table.name,
    log.name="BUILD-LOG-18-20.txt")
