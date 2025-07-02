if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
library(tidyverse)
library(arrow)
library(aws.s3)

# Corretivos/Preventivos
analitico <- s3read_using(FUN = arrow::read_parquet,
             object = "tt_sgi_atendimento_atendimentos_prazo.parquet",
             bucket = "automacao-conecta") %>% 
  filter(status_at %in% c('Atendido',
                          'Atendimento relacionado',
                          'Encontrado normal'),
         !grepl("ronda|interna", equipe, ignore.case = TRUE)) %>% 
         #data_atendimento >= '2025-01-01', data_atendimento <= '2025-05-31') %>%
  mutate(origem_da_ocorrencia = case_when(
         origem_da_ocorrencia %in% c("Ronda própria", "Sem origem definida") ~ "Preventivo",
        !origem_da_ocorrencia %in% c("Ronda própria", "Sem origem definida") ~ "Corretivo"
      )) 


write.csv(analitico,'analitico_atendimentos_prev_cor.csv')

#arrow::write_parquet(analitico, "analitico_atendimentos_prev_cor.parquet")



# UPLOAD
put_object(
    file = "analitico_atendimentos_prev_cor.csv",
    object = "analitico_atendimentos_prev_cor.csv",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )

