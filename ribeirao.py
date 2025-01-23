install.packages("base64enc")
library(base64enc)

library(httr)
library(jsonlite)
library(janitor)
library(tidyverse)
library(aws.s3)
library(arrow)
credenciais_rib <- paste0(Sys.getenv("USERNAME_RIB"), ":", Sys.getenv("PASSWORD_RIB")) %>%
      base64_enc() %>% 
      paste("Basic", .)

credenciais <- paste0(Sys.getenv("USERNAME"), ":", Sys.getenv("Ihavemacbook13?")) %>%
      base64_enc() %>% 
      paste("Basic", .)

`%!in%` <- Negate(`%in%`) 
print(credenciais_rib)
print(credenciais)




at_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){


corpo_requisicao <- list(
  CMD_ID_PARQUE_SERVICO="1",
  CMD_DATA_INICIO="01/03/2023"
  
)

  response <- POST(
     url,
     add_headers(
      `Authorization` = credenciais_rib,
      `Accept-Encoding` = "gzip"
    ),
      body = corpo_requisicao,
      encode = "json"
  )
  print(fromJSON(content(response, "text")) )
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  dados %>% glimpse

  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  atendimentos <- dados %>% 
  janitor::clean_names() %>% 
  select(-endereco) %>% 
  rename(endereco = nome_logradouro,
         lat = latitude_total_ponto,
         lon = longitude_total_ponto,
         equipe = desc_equipe,
         atendimento = desc_status_atendimento_ps,
         motivo = desc_motivo_atendimento_ps,
         no_atendimento = id_atendimento_ps,
         protocolo = numero_protocolo,
         tipo_de_ocorrencia = desc_tipo_ocorrencia) %>% 
  mutate(
    data_atendimento = as.Date(data_atendimento, "%d/%m/%Y"),
    semana_marco = week(data_atendimento) - week(as.Date("2023-02-25")),
    mes = month(data_atendimento),
    mes = case_when(
      mes == 1 ~ "Janeiro",
      mes == 2 ~ "Fevereiro",
      mes == 3 ~ "Março",
      mes == 4 ~ "Abril",
      mes == 5 ~ "Maio",
      mes == 6 ~ "Junho",
      mes == 7 ~ "Julho",
      mes == 8 ~ "Agosto",
      mes == 9 ~ "Setembro",
      mes == 10 ~ "Outubro",
      mes == 11 ~ "Novembro",
      mes == 12 ~ "Dezembro"
    ),
    mes = factor(mes, levels = c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")),
    lat = as.numeric(str_replace(lat, ",", ".")),
    lon = as.numeric(str_replace(lon, ",", "."))
  ) %>%
  filter(atendimento %!in% c("MOD: RETRABALHO", "MOD: Atendido")) %>%
  replace_na(list(motivo = "Não informado", tipo_de_ocorrencia = "Não informado")) %>%
  mutate(hora = hms(hora_inicio),
         hora_inicio = as.character(hora_inicio),
         hora_conclusao = as.character(hora_conclusao)) %>%
  mutate(data_hora = case_when(
    hora <= hms("06:00:00") ~ data_atendimento - 1,
    TRUE ~ data_atendimento
  ),
  dia_semana = wday(data_hora, label = TRUE),
  dia_semana = case_when(
    dia_semana %in% c("dom", "Sun") ~ "Dom",
    dia_semana %in% c("seg", "Mon") ~ "Seg",
    dia_semana %in% c("ter", "Tue") ~ "Ter",
    dia_semana %in% c("qua", "Wed") ~ "Qua",
    dia_semana %in% c("qui", "Thu") ~ "Qui",
    dia_semana %in% c("sex", "Fri") ~ "Sex",
    dia_semana %in% c("sab", "Sat") ~ "Sab"
  ),
  semana = week(data_hora) - week(floor_date(data_hora, "month")) + 1
  ) %>% 
  select(no_atendimento, protocolo, tipo_de_ocorrencia, atendimento, motivo, lat, lon, nome_bairro, endereco, data_atendimento, hora_inicio, hora_conclusao, equipe, semana_marco, mes, hora, data_hora, dia_semana, semana) 

  
  arrow::write_parquet(atendimentos, "tt_atendimentos_rib.parquet")
  
  put_object(
    file = "tt_atendimentos_rib.parquet",
    object = "tt_atendimentos_rib.parquet",
    bucket = "automacao-conecta",
    region = "sa-east-1"
  )
  
}


at_rib_extrai_json_api(nome = "Atendimentos",
                   raiz_1 = "PONTOS_ATENDIDOS",
                   raiz_2 = "PONTO_ATENDIDO",
                   url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarAtendimentoPontoServico.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_INICIO=01/01/2021&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8"
) 
print('Atendimentos Rib - Ok')
