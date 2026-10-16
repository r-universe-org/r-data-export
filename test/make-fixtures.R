# Regenerate test fixtures: Rscript test/make-fixtures.R
setwd("test/fixtures")
small <- head(iris, 10)
save(small, file = "small.rda")
# ~240MB in memory but tiny on disk
big <- data.frame(x = numeric(3e7))
save(big, file = "big.rda", compress = "xz")
notdf <- list(a = 1, b = quote(x + y))
save(notdf, file = "notdf.rda")
# Lazydata database as found in installed packages
env <- new.env()
assign("cars2", head(cars, 5), envir = env)
assign("letters2", data.frame(letter = letters[1:3]), envir = env)
tools:::makeLazyLoadDB(env, "Rdata")
