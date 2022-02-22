library(ggplot2)
library(tidyverse)
library(chron)

setwd("~/git/murcia-air/r")
source("mapR.R")

df <- read.csv(file="../data/san-basilio-mean-daily-2022-2023.csv", header=TRUE, sep=",")

df['DATE'] <- as.Date(df[,'DATE'], format="%d/%m/%Y")
df$PM10_Q <- ordered(df$PM10_Q, levels=c("Buena","Razonablemente buena","Regular","Desfavorable","Muy desfavorable", "Extremandamente desfavorable"))

calendarHeat(df$DATE,df$PM10,ncolors = 5, color="g2r")
