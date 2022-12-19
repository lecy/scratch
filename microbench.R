start.time <- Sys.time()


X <- matrix(rnorm(50000*1000), 50000, 1000)
b <- sample(1:1000, 1000)
y <- runif(1) + X %*% b + rnorm(50000)
model <- lm(y ~ X)


end.time <- Sys.time()

end.time - start.time

print( difftime( start.time, end.time, units="minutes") )
print( difftime( start.time, end.time, units="hours") )
print( difftime( start.time, end.time, units="days") )
