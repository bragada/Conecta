if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("googlesheets4", quietly = TRUE)) install.packages("googlesheets4")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("googlesheets4")

library(tidyverse)
library(arrow)
library(aws.s3)
library(googlesheets4)
library(gargle)
library(httr)

gs4_auth(path = "sa.json")

# Corretivos/Preventivos
analitico <- s3read_using(FUN = arrow::read_parquet,
             object = "tt_sgi_atendimento_atendimentos_prazo.parquet",
             bucket = "automacao-conecta") %>% 
  filter(status_at %in% c('Atendido',
                          'Atendimento relacionado',
                          'Encontrado normal'),
         !grepl("ronda|interna", equipe, ignore.case = TRUE),
         data_atendimento >= '2025-01-01') %>% 
         #data_atendimento >= '2025-01-01', data_atendimento <= '2025-05-31') %>%
  mutate(origem_da_ocorrencia = case_when(
         origem_da_ocorrencia %in% c("Ronda própria", "Sem origem definida") ~ "Preventivo",
        !origem_da_ocorrencia %in% c("Ronda própria", "Sem origem definida") ~ "Corretivo"),
        dt_movimento = data_atendimento  %>% ceiling_date("month") - 1)


#write.csv(analitico,'analitico_atendimentos_prev_cor.csv')


# Defina o ID ou a URL da sua planilha no Google Sheets
gsheet_url <- "[https://docs.google.com/spreadsheets/d/10p-WT-sRI6UHXa1XRNKmz3Q-Fbby8pnJG3fnnLvKke8/edit?gid=0#gid=0]"

# Autenticação (se necessário)
# gs4_auth(email = "seu-email@gmail.com")

# Escrever os dados na planilha do Google Sheets
sheet_write(analitico, gsheet_url, sheet = "Analitico")
#arrow::write_parquet(analitico, "analitico_atendimentos_prev_cor.parquet")



# UPLOAD
#put_object(
#    file = "analitico_atendimentos_prev_cor.csv",
#    object = "analitico_atendimentos_prev_cor.csv",
#    bucket = "automacao-conecta",
#    region = 'sa-east-1'
#  )

