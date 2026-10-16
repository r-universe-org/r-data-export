# Evaluated in a dedicated environment inside each webR worker at startup.
# Adapted from webr-bundle/datatool, which copied the data readers from utils::data().

convert <- function(input, name, format, output){
  env <- load_data_env(input, name)
  if(!exists(name, envir = env, inherits = FALSE))
    stop(sprintf("Dataset '%s' not found in %s", name, basename(input)))
  save_data(name, env, format, output)
  rm(env)
  file.size(output)
}

load_data_env <- function(filename, name){
  env <- new.env(parent = emptyenv())
  ext <- tolower(file_ext(filename))
  switch(ext,
         rdb =, rdx =
           lazyLoad(sub('\\.rd.$', '', filename), envir = env, filter = function(x) x == name),
         rdata = , rda =
           load(filename, envir = env),
         rds =
           assign(name, readRDS(filename), envir = env),
         txt = , tab = , tab.gz = , tab.bz2 = , tab.xz = , txt.gz = , txt.bz2 = , txt.xz =
           assign(name, utils::read.table(filename, header = TRUE, as.is = FALSE), envir = env),
         csv = , csv.gz = , csv.bz2 = , csv.xz =
           assign(name, utils::read.table(filename, header = TRUE, sep = ";", as.is = FALSE), envir = env),
         stop("Unsupported data type: ", basename(filename)))
  env
}

as_table <- function(df){
  if(is.data.frame(df))
    return(df)
  if(is.matrix(df) || is.table(df))
    return(as.data.frame(df))
  stop(sprintf("Cannot export object of class '%s' to a table", paste(class(df), collapse = "/")))
}

save_data <- function(name, env, format, output){
  df <- get(name, envir = env, inherits = FALSE)
  # Resolve lazyload promise so errors happen here and not in the writer
  force(df)
  switch(format,
         csv = data.table::fwrite(as_table(df), output),
         csv.gz = data.table::fwrite(as_table(df), output, compress = 'gzip'),
         xlsx = writexl::write_xlsx(as_table(df), output),
         json = jsonlite::write_json(df, output, force = TRUE),
         ndjson = jsonlite::stream_out(as_table(df), file(output), force = TRUE, verbose = FALSE),
         rds = saveRDS(df, output),
         rda = save(list = name, envir = env, file = output),
         stop("Unsupported format: ", format)
  )
}

file_ext <- function(x) {
  db <- grepl("\\.[^.]+\\.(gz|bz2|xz)$", x)
  ans <- sub(".*\\.", "", x)
  ans[db] <- sub(".*\\.([^.]+\\.)(gz|bz2|xz)$", "\\1\\2", x[db])
  ans
}
