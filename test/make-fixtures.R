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
longstr <- data.frame(id = 1:2, txt = c(strrep("x", 40000), "short"))
save(longstr, file = "longstr.rda")
# list columns, like dplyr::starwars, and other types that xlsx can't hold
listcol <- data.frame(id = 1:3)
listcol$films <- list(c("A New Hope", "Return of the Jedi"), character(), "Solo")
listcol$nested <- list(list(a = 1), NULL, data.frame(x = 1:2))
listcol$cplx <- complex(real = 1:3, imaginary = 1)
save(listcol, file = "listcol.rda")
# latin1 bytes marked as UTF-8, like ICD10gm::icd_meta_codes
badutf8 <- data.frame(label = c("ok", "S\xe9zary-Syndrom", NA))
Encoding(badutf8$label) <- "UTF-8"
save(badutf8, file = "badutf8.rda")
