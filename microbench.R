start_time <- Sys.time()


X <- matrix(rnorm(50000*1000), 50000, 1000)
b <- sample(1:1000, 1000)
y <- runif(1) + X %*% b + rnorm(50000)
model <- lm(y ~ X)


end_time <- Sys.time()

end_time - start_time


