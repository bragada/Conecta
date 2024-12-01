#install.packages(c("httr", "jsonlite", "janitor", "tidyverse", "aws.s3", "arrow"))
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
print("vamo vê o que vai dar")
at_extrai_json_api <- function(nome,url,raiz_1,raiz_2){


  corpo_requisicao <- list(
    CMD_ID_STATUS_SOLICITACAO=-1,
    CMD_IDS_PARQUE_SERVICO="1,2",
    CMD_DATA_RECLAMACAO="01/03/2023",
    CMD_APENAS_EM_ABERTO=0
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
  
  dados <- fromJSON(content(response, "text"))
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
  
  
  arrow::write_parquet(atendimentos, "tt_atendimentos.parquet")
  
  put_object(
    file = "tt_atendimentos.parquet",
    object = "tt_atendimentos.parquet",
    bucket = "automacao-conecta",
    region = "sa-east-1"
  )
  
}


at_extrai_json_api(nome = "Atendimentos",
                   raiz_1 = "PONTOS_ATENDIDOS",
                   raiz_2 = "PONTO_ATENDIDO",
                   url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarAtendimentoPontoServico.json?CMD_ID_PARQUE_SERVICO=2&CMD_DATA_INICIO=01/03/2023&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
) 
print('Atendimentos - Ok')




# Solicitações ----
sol_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
  corpo_requisicao <- list(
  CMD_ID_PARQUE_SERVICO = 2,
  CMD_DATA_INICIO = "01/03/2023"
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
  
  dados <- fromJSON(content(response, "text"))
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
  
  solicitacoes <- dados %>% 
    clean_names() %>%
    select(protocolo = numero_protocolo,
           data_reclamacao,
           status = desc_status_solicitacao,
           tempo_restante = desc_prazo_restante,
           data_reclamacao,
           id_ocorrencia,
           possui_atendimento_anterior,
           endereco_livre_solicitacao,
           origem_ocorrencia = desc_tipo_origem_solicitacao,
           pontos
    ) %>% 
    mutate(data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
           semana_marco = week(data_reclamacao)-week(as.Date("2023-02-25")),
           mes = month(data_reclamacao),
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
             mes == 12 ~ "Dezembro",
           ),
           mes = factor(mes,levels = c("Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro")),
           dia_semana = wday(data_reclamacao,label = T),
           dia_semana = case_when(
             dia_semana %in% c("dom","Sun") ~ "Dom",
             dia_semana %in% c("seg","Mon") ~ "Seg",
             dia_semana %in% c("ter","Tue") ~ "Ter",
             dia_semana %in% c("qua","Wed") ~ "Qua",
             dia_semana %in% c("qui","Thu") ~ "Qui",
             dia_semana %in% c("sex","Fri") ~ "Sex",
             dia_semana %in% c("sab","Sat") ~ "Sab"
             
           ),
           semana = week(data_reclamacao) - week(floor_date(data_reclamacao,"month")) +1) 
  
  
  arrow::write_parquet(solicitacoes, "tt_solicitacoes.parquet")
  
  put_object(
    file = "tt_solicitacoes.parquet",
    object = "tt_solicitacoes.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

sol_extrai_json_api(nome = "Solicitações",
                    raiz_1 = "SOLICITACOES",
                    raiz_2 = "SOLICITACAO",
                    url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/Solicitacoes.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=1,2&CMD_DATA_RECLAMACAO=01/03/2023&CMD_APENAS_EM_ABERTO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
) 
print('Solicitações - Ok')

# ----


# Ocorrencias/Solicitacoes Pendentes Realizadas ----
osp_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
corpo_requisicao <- list(
  CMD_ID_PARQUE_SERVICO = "[1,2]",
  CMD_AGRUPAMENTO = "SOLICITACAO_PONTO_SERVICO",
  CMD_STATUS = "PENDENTES",
  CMD_ORIGEM_ATENDIMENTO = "TODOS",
  CMD_TIPO_SOLICITACAO = "TODOS",
  CMD_DATA_INICIO = format(Sys.Date() - 90, "%d/%m/%Y"),
  CMD_DATA_FIM = format(Sys.Date(), "%d/%m/%Y")
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
  
  dados <- fromJSON(content(response, "text"))
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
  
  osp <<- dados %>% 
    clean_names() %>% 
    select(id_ocorrencia=id_ocorrencia,
           protocolo = numero_protocolo,
           tipo_ocorrencia = desc_tipo_ocorrencia,
           status = descricao_status,
           origem_ocorrencia = desc_tipo_origem_ocorrencia,
           prioridade = sigla_prioridade_ponto_ocorr) %>% 
    distinct()
  
  
  
  
  arrow::write_parquet(osp, "tt_osp.parquet")
  #
  put_object(
    file = "tt_osp.parquet",
    object = "tt_osp.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

osp_extrai_json_api(nome = "Ocorrencias/Solicitacoes Pendentes Realizadas ",
                    raiz_1 = "OCORRENCIAS_SOLICITACOES",
                    raiz_2 = "OCORRENCIA_SOLICITACAO",
                    url = paste0("https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarOcorrenciasSolicitacoesPendentesRealizadas.json?CMD_ID_PARQUE_SERVICO=[1,2]&CMD_AGRUPAMENTO=SOLICITACAO_PONTO_SERVICO&CMD_STATUS=PENDENTES&CMD_ORIGEM_ATENDIMENTO=TODOS&CMD_TIPO_SOLICITACAO=TODOS&CMD_DATA_INICIO=",format(Sys.Date()-90,"%d/%m/%Y"),"&CMD_DATA_FIM=",format(Sys.Date(),"%d/%m/%Y"),"&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
)
print('  Ocorrencias/Solicitacoes - Ok')   
# ----


# Painel Ocorrências ----
p_oc_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
corpo_requisicao <- list(
  CMD_IDS_PARQUE_SERVICO = 2,
  CMD_DENTRO_DE_AREA = -1,
  auth_token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
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
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    print("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  
  if (length(dados) <= 3) {
    print("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  osp <- s3read_using(FUN = arrow::read_parquet,
                      object = "tt_osp.parquet",
                      bucket = "automacao-conecta"
  )
  
  p_oc <- dados %>% 
    clean_names() %>% 
    select(
      protocolo = numero_protocolo ,
      tipo_de_ocorrencia = desc_tipo_origem_ocorrencia,
      #limite_atendimento,
      bairro = nome_bairro,
      endereco = endereco_livre,
      id_ocorrencia,
      data_reclamacao,
      endereco_livre = nome_logradouro_completo,
      data_limite_atendimento,
      hora_limite_atendimento,
      latitude_total,
      longitude_total,
      possui_atendimento_anterior,
      quant_solicitacoes_vinculadas
    ) %>% 
    mutate(
      limite_atendimento =  as.POSIXct(strptime(paste(data_limite_atendimento,hora_limite_atendimento),"%d/%m/%Y %H:%M")),
      data_limite_para_atendimento = limite_atendimento,
      #recebida =  as.POSIXct(strptime(recebida,"%d/%m/%Y %H:%M")),
      data_limite = limite_atendimento,
      dif = as.numeric(round(difftime(data_limite, as.POSIXct(Sys.time(),"GMT"),units = "hours"),0)),
      data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
      data_limite_atendimento = as.Date(data_limite_atendimento,"%d/%m/%Y"),
      dias_prazo = as.numeric(data_limite_atendimento - Sys.Date()),
      atrasado = ifelse(dias_prazo < 0, "Atrasada","No Prazo"),
      lat=as.numeric(str_replace(latitude_total,",",".")),
      lon=as.numeric(str_replace(longitude_total,",","."))) %>% 
    #rename(lat=latitude_total,lon=longitude_total)  %>% 
    mutate(
      cor_atraso = case_when(
        dias_prazo >= 0 ~ "darkgreen",
        TRUE ~ "red"
      )) %>% 
    left_join(
      osp,by = c("protocolo","id_ocorrencia")
    ) %>% 
    select(-tipo_de_ocorrencia) %>% 
    rename(tipo_de_ocorrencia = tipo_ocorrencia)
  
  
  
  arrow::write_parquet(p_oc, "tt_painel_ocorrencias.parquet")
  
  put_object(
    file = "tt_painel_ocorrencias.parquet",
    object = "tt_painel_ocorrencias.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

p_oc_extrai_json_api(nome = "Painel de Ocorrências",
                     raiz_1 = "PONTOS_SERVICO",
                     raiz_2 = "PONTO_SERVICO",
                     url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/PaineldeOcorrencias.json?CMD_IDS_PARQUE_SERVICO=2&CMD_DENTRO_DE_AREA=-1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print(' Painel Ocorrências - Ok')

# ----



