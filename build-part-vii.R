library( dplyr )
library( irs990efile )
library( jsonlite )

# https://www.dropbox.com/s/pdfls0e1jz9trvi/index-nccs.rds?dl=0

index <- readRDS( "index-nccs.rds" )
index <- dplyr::filter( index, FormType %in% c("990","990EZ") )



########################
######################## FIX FUNCTIONS
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


build_tables <- function( index=NULL, years=NULL, table.name )
{
  
  start.build.time <- Sys.time()
  
  session.info <- sessionInfo()
  dump( list="session.info", file="SESSION-INFO.R" )
  
  zz <- file( "BUILD-LOG.txt", open = "wt" )
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

  bind_data( years=years )
  
  end.build.time <- Sys.time()
  print( paste0( "DATABASE BUILD FINISH TIME: ", Sys.time() ) )
  print( paste0( "TOTAL BUILD TIME: ", round( difftime( end.build.time, start.build.time, units="hours" ), 2 ), " HOURS" ) )

  sink( type="message" )
  sink()      # close sink
  close(zz)   # close connection
  file.show( "BUILD-LOG.txt" )
        
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

    print( paste0( "Batch ", i ) )
    print( end.time - start.time )
    # print( paste( "There are ", length(failed.urls), " failed XML URLs to re-try." ) )
  
    ## add random string so filenames not duplicated
    ## when using parallelization 
    # time <- format(Sys.time(), "%b-%d-%Y-%Hh-%Mm")
    # rand <- paste( sample(LETTERS,5), collapse="" )
    # time <- paste0( "time-", time, "-", rand  )

    b.num <- substr( 10000 + i, 2, 5 ) 

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

  dir.create( "COMPILED" )  
  setwd( year ) 
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

  setwd( "../COMPILED" )
 
   
  write.csv( d, paste0( "PARTVII-", year, ".csv", row.names=F ) )
  saveRDS(   d, paste0( "PARTVII-", year, ".rds" )  )    

  setwd( ".." )       
}




#########################################
#########################################   F9-P07-T01-COMPENSATION
#########################################


find_group_names( table.name="F9-P07-T01-COMPENSATION" )

# map to standardize variable names across table versions 
v.map <- get_var_map( table.name="F9-P07-T01-COMPENSATION" )
x1 <- c("F9_07_COMP_DTK_COMP_OTH","F9_07_COMP_DTK_COMP_OTH")
x2 <- c("ExpenseAccountOtherAllwncAmt","ExpenseAccountOtherAllowances")
d.add <- data.frame( VARIABLE=x1, XSD_VARNAME=x2 )
v.map <- bind_rows( v.map, d.add )


table.name <- "F9-P07-T01-COMPENSATION"

years <- c( 2009:2020 ) %>% as.character()



build_tables( 
    index=index, 
    years=years, 
    table.name="F9-P07-T01-COMPENSATION" )










#####################################
#####################################




results.list <- list()

for( i in 1:length(test.urls) )
{
  url <- test.urls[i]
  try( results.list[[i]] <- build_rdb_table( url, table.name, v.map ) )
}


df <- dplyr::bind_rows( results.list )





##########################
##########################




split_index <- function( index, group.size=1000 )
{
  urls <- index$URL
  f <- ( ( 1 : length(urls) ) + group.size - 1 ) %/% group.size
  f <- paste0( "g", f )
  f <- factor( f, levels=unique(f) )
  url.list <- split( urls, f )
  return( url.list )
}






build_year <- function( year )
{


  dir.create( year )
  setwd( year )

  index.sub <- dplyr::filter( index, TaxYear == year )
  split.index <- split_index( index.sub )

  results.list <- list()
  failed.urls <- NULL


  for( i in 1:length(test.urls) )
  {


    urls <- split.index[[i]]
    

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




    print( paste( "There are ", length(failed.urls), " failed XML URLs to re-try." ) )
  
    ## add random string so filenames not duplicated
    ## when using parallelization 
    # time <- format(Sys.time(), "%b-%d-%Y-%Hh-%Mm")
    # rand <- paste( sample(LETTERS,5), collapse="" )
    # time <- paste0( "time-", time, "-", rand  )

    b.num <- substr( 10000 + i, 2, 5 ) 

    df <- dplyr::bind_rows( results.list )
    saveRDS( df, paste0( "batch", "-", b.num, ".rds" ) )
    write.csv( df, paste0( "batch", "-", b.num, ".csv" ), row.names=F )
  }


  failed.urls2 <- unlist( failed.urls )
  dump( list="failed.urls2", file="FAILED-URLS.R" )  

  setwd( ".." )

  print( paste0( "YEAR ", year, " COMPLETE" ) )

  # return( failed.urls2 )

}




years <- c( 2009:2020 ) %>% as.character()

for( k in years )
{
  build_year( k )
}

