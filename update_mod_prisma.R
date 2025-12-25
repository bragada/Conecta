if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("googlesheets4", quietly = TRUE)) install.packages("googlesheets4")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("googlesheets4")

library(googlesheets4)
library(gargle)

gs4_auth(path = "sa.json")
install.packages("base64enc")
library(base64enc)

library(httr)
library(jsonlite)
library(janitor)
library(tidyverse)
library(aws.s3)
library(arrow)


credenciais <- paste0(Sys.getenv("USERNAME"), ":", Sys.getenv("PASSWORD")) %>%
      base64_enc() %>% 
      paste("Basic", .)

`%!in%` <- Negate(`%in%`) 



########################################################################################
mod_lum_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

corpo_requisicao <- list(
  CMD_PARQUE_SERVICO = 2,
  CMD_MODERNIZACAO = 2,
  CMD_TIPO_CALCULO = 1
)

 response <- POST(
     url,
     add_headers(
      `Authorization` = credenciais,
      `Accept-Encoding` = "gzip"
    ),
      body = corpo_requisicao,
      encode = "json"
  )    
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  mod_lum <- dados %>% clean_names() %>% mutate(data_mod = as.Date(data_ultima_mod)) %>% distinct(id_ponto_servico)
  
  
  
  # UPLOAD SHEET
  gsheet_url <- "[https://docs.google.com/spreadsheets/d/14wp-xTzqIonTzw6Y1sIq1BCffqOEfe45GIawx15ak5Q/edit?gid=0#gid=0]"
  
  # Autenticação (se necessário)
  # gs4_auth(email = "seu-email@gmail.com")
  
  # Escrever os dados na planilha do Google Sheets
  sheet_write(mod_lum, gsheet_url, sheet = "id_ponto_servico")
  
}

mod_lum_extrai_json_api(nome = "mod_lum",
                      raiz_1 = "PONTOS_MODERNIZACAO",
                      raiz_2 = "PONTO_MODERNIZACAO",
                      url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/webservice-consultarpontosmodernizacaocompleto.json?CMD_IDS_PARQUE_SERVICO=2&CMD_PAGE_SIZE=0&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=1"
)
print('  Mod Lum - Ok')     



