install.packages(c("httr", "jsonlite", "janitor", "tidyverse", "aws.s3", "arrow"))

library(httr)
library(jsonlite)
library(janitor)
library(tidyverse)
library(aws.s3)
library(arrow)


`%!in%` <- Negate(`%in%`) 


# Atendimentos ----
at_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
   
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarAtendimentoPontoServico.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_INICIO=01/01/2021&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['PONTOS_ATENDIDOS']] %>%
  #  .[['PONTO_ATENDIDO']] %>% clean_names()
  
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
    rename(
      equipe = any_of("desc_equipe"),
       tipo_de_ocorrencia = any_of("desc_tipo_ocorrencia"),
       bairro = any_of("nome_bairro"),
       endereco = any_of("endereco_livre"),
       protocolo = any_of("numero_protocolo"),
       id_ordem_servico = any_of("id_ordem_servico"),
       data_reclamacao = any_of("data_reclamacao"),
       hora_limite_atendimento = any_of("hora_limite_atendimento"),
       data_limite_atendimento = any_of("data_limite_atendimento"),
       latitude_total = any_of("latitude_total"),
       longitude_total = any_of("longitude_total")) %>%
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
    region = 'sa-east-1'
  )
  
}


at_rib_extrai_json_api(nome = "Atendimentos",
                   raiz_1 = "PONTOS_ATENDIDOS",
                   raiz_2 = "PONTO_ATENDIDO",
                   url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarAtendimentoPontoServico.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_INICIO=01/01/2021&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8"
) 
print('Atendimentos - Ok')

# ----

# Solicitações ----
sol_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/Solicitacoes.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_RECLAMACAO=01/03/2023&CMD_APENAS_EM_ABERTO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['SOLICITACOES']] %>%
  #  .[['SOLICITACAO']] %>% clean_names()
  
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
    select(  
       protocolo = any_of("numero_protocolo"),
       status = any_of("desc_status_solicitacao"),
       tempo_restante = any_of("desc_prazo_restante"),
       id_ocorrencia = any_of("id_ocorrencia"),
       possui_atendimento_anterior = any_of("possui_atendimento_anterior"),
       endereco_livre_solicitacao = any_of("endereco_livre_solicitacao"),
       origem_ocorrencia = any_of("desc_tipo_origem_solicitacao"),
       pontos = any_of("pontos")
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

sol_rib_extrai_json_api(nome = "Solicitações",
                    raiz_1 = "SOLICITACOES",
                    raiz_2 = "SOLICITACAO",
                    url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/Solicitacoes.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_RECLAMACAO=01/03/2023&CMD_APENAS_EM_ABERTO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8"
) 
print('Solicitações - Ok')

# ----

# Ocorrencias/Solicitacoes Pendentes Realizadas ----
osp_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
    
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarOcorrenciasSolicitacoesPendentesRealizadas.json?CMD_ID_PARQUE_SERVICO=[1]&CMD_AGRUPAMENTO=SOLICITACAO_PONTO_SERVICO&CMD_STATUS=TODOS&CMD_ORIGEM_ATENDIMENTO=TODOS&CMD_TIPO_SOLICITACAO=TODOS&CMD_DATA_INICIO=14/08/2024&CMD_DATA_FIM=14/08/2024&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['OCORRENCIAS_SOLICITACOES']] %>%
  #  .[['OCORRENCIA_SOLICITACAO']] %>% clean_names()
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  osp <- dados %>% 
    clean_names() %>% 
    select(
       id_ocorrencia = any_of("id_ocorrencia"),
       protocolo = any_of("numero_protocolo"),
       tipo_ocorrencia = any_of("desc_tipo_ocorrencia"),
       status = any_of("descricao_status"),
       origem_ocorrencia = any_of("desc_tipo_origem_ocorrencia"),
       prioridade = any_of("sigla_prioridade_ponto_ocorr")) %>% 
    distinct()
  
  
  
  
  arrow::write_parquet(osp, "tt_osp_rib.parquet")
  #
  put_object(
    file = "tt_osp_rib.parquet",
    object = "tt_osp_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

osp_rib_extrai_json_api(nome = "Ocorrencias/Solicitacoes Pendentes Realizadas ",
                    raiz_1 = "OCORRENCIAS_SOLICITACOES",
                    raiz_2 = "OCORRENCIA_SOLICITACAO",
                    url = paste0("https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarOcorrenciasSolicitacoesPendentesRealizadas.json?CMD_ID_PARQUE_SERVICO=[1]&CMD_AGRUPAMENTO=SOLICITACAO_PONTO_SERVICO&CMD_STATUS=TODOS&CMD_ORIGEM_ATENDIMENTO=TODOS&CMD_TIPO_SOLICITACAO=TODOS&CMD_DATA_INICIO=14/08/2024&CMD_DATA_FIM=14/08/2024&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
)
print('Ocorrencias/Solicitacoes - Ok')   
# ----

# Painel Ocorrências ----
p_oc_rib_extrai_json_api_rib <- function(nome,url,raiz_1,raiz_2){
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    print("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/PaineldeOcorrencias.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DENTRO_DE_AREA=-1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['PONTOS_SERVICO']] %>%
  #  .[['PONTO_SERVICO']] %>% clean_names()
  
  if (length(dados) <= 10) {
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
 protocolo = any_of("numero_protocolo"),
    tipo_de_ocorrencia = any_of("desc_tipo_origem_ocorrencia"),
    bairro = any_of("nome_bairro"),
    endereco = any_of("endereco_livre"),
    id_ocorrencia = any_of("id_ocorrencia"),
    data_reclamacao = any_of("data_reclamacao"),
    endereco_livre = any_of("nome_logradouro_completo"),
    data_limite_atendimento = any_of("data_limite_atendimento"),
    hora_limite_atendimento = any_of("hora_limite_atendimento"),
    latitude_total = any_of("latitude_total"),
    longitude_total = any_of("longitude_total"),
    possui_atendimento_anterior = any_of("possui_atendimento_anterior"),
    quant_solicitacoes_vinculadas = any_of("quant_solicitacoes_vinculadas")
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
  
  
  
  arrow::write_parquet(p_oc, "tt_painel_ocorrencias_rib.parquet")
  
  put_object(
    file = "tt_painel_ocorrencias_rib.parquet",
    object = "tt_painel_ocorrencias_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

p_oc_rib_extrai_json_api_rib(nome = "Painel de Ocorrências",
                     raiz_1 = "PONTOS_SERVICO",
                     raiz_2 = "PONTO_SERVICO",
                     url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/PaineldeOcorrencias.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DENTRO_DE_AREA=-1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print('Painel Ocorrências - Ok')

# ----

# Painel Monitoramento ----
p_moni_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
 
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]] %>% clean_names()
  
  #dados <- fromJSON(content(GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPontosServicoOcorrenciaAndamentoEquipe.json?CMD_ID_PARQUE_SERVICO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['PONTOS_SERVICO']] %>%
  #  .[['PONTO_SERVICO']] %>% clean_names()
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  valid_bairro <-  ifelse("nome_bairro" %in% names(dados),"nome_bairro")
  
  
  
  p_moni <- dados %>% 
    clean_names() %>%
    mutate(
      nome_bairro = ifelse("nome_bairro" %in% names(dados),nome_bairro,"Sem Informação"),
      latitude_total = ifelse("latitude_total" %in% names(dados),latitude_total,NA),
      longitude_total = ifelse("longitude_total" %in% names(dados),latitude_total,NA)
    ) %>% 
    select(
    equipe = any_of("desc_equipe"),
    tipo_de_ocorrencia = any_of("desc_tipo_ocorrencia"),
    bairro = any_of("nome_bairro"),
    endereco = any_of("endereco_livre"),
    protocolo = any_of("numero_protocolo"),
    id_ordem_servico = any_of("id_ordem_servico"),
    data_reclamacao = any_of("data_reclamacao"),
    hora_limite_atendimento = any_of("hora_limite_atendimento"),
    data_limite_atendimento = any_of("data_limite_atendimento"),
    latitude_total = any_of("latitude_total"),
    longitude_total = any_of("longitude_total")
    ) %>% 
    mutate(
      data_limite_para_atendimento = as.POSIXct(strptime(paste(data_limite_atendimento,hora_limite_atendimento),"%d/%m/%Y %H:%M")),
      #recebida =  as.POSIXct(strptime(recebida,"%d/%m/%Y %H:%M")),
      data_limite =data_limite_para_atendimento,
      dif = as.numeric(round(difftime(data_limite, as.POSIXct(Sys.time(),"GMT"),units = "hours"),0)),
      data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
      data_limite_atendimento = as.Date(data_limite_atendimento,"%d/%m/%Y"),
      dias_prazo = as.numeric(data_limite_atendimento - Sys.Date()),
      atrasado = ifelse(dias_prazo < 0, "Atrasada","No Prazo"),
      lat=as.numeric(str_replace(latitude_total,",",".")),
      lon=as.numeric(str_replace(longitude_total,",","."))
    ) %>% 
    mutate(
      cor_atraso = case_when(
        dias_prazo >= 0 ~ "darkgreen",
        TRUE ~ "red"
      )) 
  
  
  
  arrow::write_parquet(p_moni, "tt_painel_monitoramento_rib.parquet")
  
  put_object(
    file = "tt_painel_monitoramento_rib.parquet",
    object = "tt_painel_monitoramento_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

p_moni_rib_extrai_json_api(nome = "Painel de Monitoramento",
                       raiz_1 = "PONTOS_SERVICO",
                       raiz_2 = "PONTO_SERVICO",
                       url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPontosServicoOcorrenciaAndamentoEquipe.json?CMD_ID_PARQUE_SERVICO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print(' Painel Monitoramento - Ok')

# ----

# Ordens de Serviço ----
os_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  

  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  #dados <- fromJSON(content(GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/Ordensdeservico.json?CMD_ID_PARQUE_SERVICO=1&CMD_DATA_INICIAL=01/07/2024&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['ORDENS_SERVICO']] %>%
  #  .[['ORDEM_SERVICO']] %>% clean_names()
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  os <- dados %>% 
    clean_names() %>%
    select(
     id_ordem_servico = any_of("id_ordem_servico"),
    data = any_of("data"),
    prazo = any_of("prazo_restante"),
    status = any_of("desc_status_ordem_servico"),
    desc_tipo_ordem_servico = any_of("desc_tipo_ordem_servico"),
    equipe = any_of("desc_equipe"),
    desc_ordem_servico = any_of("desc_ordem_servico"),
    data_hora_recebido = any_of("data_hora_recebido"),
    total_pontos = any_of("total_pontos"),
    total_atendidos = any_of("total_atendidos")
    ) %>% 
    mutate(data= as.POSIXct(strptime(data,"%d/%m/%Y %H:%M")),
           prazo = as.numeric(str_replace(prazo,",",".")),
           tarefas_finalizadas = paste0(total_atendidos,"/",total_pontos),
           avanco = round(100*(total_atendidos/total_pontos),0)
    )
  
  
  
  arrow::write_parquet(os, "tt_ordens_servico_rib.parquet")
  
  put_object(
    file = "tt_ordens_servico_rib.parquet",
    object = "tt_ordens_servico_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

os_rib_extrai_json_api(nome = "Ordens de Serviço",
                   raiz_1 = "ORDENS_SERVICO",
                   raiz_2 = "ORDEM_SERVICO",
                   url="https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/Ordensdeservico.json?CMD_ID_PARQUE_SERVICO=1&CMD_DATA_INICIAL=01/07/2024&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print(' Ordens de Serviço - Ok')

# ----

# Ocorrências Autorizar ----
oa_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarOcorrenciasAutorizar.json?CMD_IDS_PARQUE_SERVICO=1&CMD_PAINEL_NOVO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['PONTOS_SERVICO']] %>%
  #  .[['PONTO_SERVICO']] %>% clean_names()
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  oa <- dados %>% 
    clean_names() %>%
    select(
      protocolo = any_of("numero_protocolo"),
    tipo_de_ocorrencia = any_of("desc_tipo_ocorrencia"),
    data_limite_de_atendimento_original = any_of("data_limite_atendimento"),
    bairro = any_of("nome_bairro"),
    endereco = any_of("nome_logradouro_completo"),
    tempo_paralisado = any_of("tempo_pendente"),
    data_reclamacao = any_of("data_reclamacao")
    ) %>% 
    mutate(data_limite_de_atendimento_original = as.Date(data_limite_de_atendimento_original,"%d/%m/%Y"),
           data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"))
  
  
  arrow::write_parquet(oa, "tt_ocorrencias_autorizar_rib.parquet")
  
  put_object(
    file = "tt_ocorrencias_autorizar_rib.parquet",
    object = "tt_ocorrencias_autorizar_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
  
  
}

oa_rib_extrai_json_api(nome = "Ocorrências Autorizar",
                   raiz_1 = "PONTOS_SERVICO",
                   raiz_2 = "PONTO_SERVICO",
                   url = "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarOcorrenciasAutorizar.json?CMD_IDS_PARQUE_SERVICO=1&CMD_PAINEL_NOVO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print('  Ocorrências Autorizar  - Ok')                
# ----

# ATENDIMENTO QUANTO AO PRAZO ----
sgi_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPrazosAtendimento.json?CMD_ID_PARQUE_SERVICO=1&CMD_DATA_INICIAL_FILTRO=01/01/2021&CMD_DATA_FINAL_FILTRO=01/01/2040&CMD_ID_SEM_REGIAO=-1&CMD_DETALHADO=1&CMD_CONFIRMADOS=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['ATENDIMENTOS']] %>%
  #  .[['ATENDIMENTO']] %>% clean_names()
  print("antes dados")
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
    print("depois dados")

  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
      print("antes sgi")

  sgi <- dados %>% 
    clean_names() %>% 
    select(
    atendimento = any_of("id_atendimento_ps"),
    prazo = any_of("data_limite_atendimento"),
    prazo_hora = any_of("hora_limite_atendimento"),
    data_atendimento = any_of("data_atendimento"),
    atendimento_hora = any_of("hora_atendimento"),
    prev_execucao_horas = any_of("previsao_execucao"),
    status = any_of("no_prazo"),
    origem_da_ocorrencia = any_of("origem_ocorrencia")
      
    ) %>% 
    #select(-x1) %>% 
    #slice(-1) %>% 
    mutate(prazo = as.Date(prazo,"%d/%m/%Y"),
           data_atendimento = as.Date(data_atendimento,"%d/%m/%Y"),
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
           mes = factor(mes,levels = c("Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro")),
           hora = hms(atendimento_hora),
           data_hora = case_when(
             hora <= hms("06:00:00") ~ data_atendimento-1,
             TRUE ~ data_atendimento
           ),
           dia_semana = wday(data_hora,label = T),
           dia_semana = case_when(
             dia_semana %in% c("dom","Sun") ~ "Dom",
             dia_semana %in% c("seg","Mon") ~ "Seg",
             dia_semana %in% c("ter","Tue") ~ "Ter",
             dia_semana %in% c("qua","Wed") ~ "Qua",
             dia_semana %in% c("qui","Thu") ~ "Qui",
             dia_semana %in% c("sex","Fri") ~ "Sex",
             dia_semana %in% c("sab","Sat") ~ "Sab"
             
           ),
           atendimento = as.character(atendimento)
    )  %>%
    filter(!is.na(data_hora)) %>% 
    left_join(
      #fst::read_fst("C:/Users/hk/HD_Externo/Conecta/atendimentos.fst") %>% 
      s3read_using(
          FUN = arrow::read_parquet,
          object = "tt_atendimentos_rib.parquet",
          bucket = "automacao-conecta"
        ) %>% 
        select(no_atendimento,equipe,status_at = atendimento) %>% 
        mutate(no_atendimento = as.character(no_atendimento))
      , by = c("atendimento" = "no_atendimento"))
  print('depois agi')
  arrow::write_parquet(sgi, "tt_sgi_atendimento_atendimentos_prazo_rib.parquet")
    print('antes put')

  put_object(
    file = "tt_sgi_atendimento_atendimentos_prazo_rib.parquet",
    object = "tt_sgi_atendimento_atendimentos_prazo_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
      print('depois put')

}

sgi_rib_extrai_json_api(nome = "ATENDIMENTO QUANTO AO PRAZO",
                    raiz_1 = "ATENDIMENTOS",
                    raiz_2 = "ATENDIMENTO",
                    url = "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPrazosAtendimento.json?CMD_ID_PARQUE_SERVICO=1&CMD_DATA_INICIAL_FILTRO=01/01/2021&CMD_DATA_FINAL_FILTRO=01/01/2040&CMD_ID_SEM_REGIAO=-1&CMD_DETALHADO=1&CMD_CONFIRMADOS=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print('  ATENDIMENTO QUANTO AO PRAZO  - Ok')                

# ----

# PONTOS MODERNIZADOS -----
mod_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPontosModernizacaoCompleto.json?CMD_IDS_PARQUE_SERVICO=1&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['PONTOS_MODERNIZACAO']] %>%
  #  .[['PONTO_MODERNIZACAO']]
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  mod <- dados %>% 
    clean_names() %>% 
    select(
 etiqueta = any_of("id_ponto_servico"),
    data_mod = any_of("data_ultima_mod"),
    hora = any_of("hora_ultima_mod"),
    equipe = any_of("equipe_ultima_mod"),
    endereco = any_of("endereco"),
    lat = any_of("latitude"),
    lon = any_of("longitude"),
    potencia_da_lampada_ultima_modernizacao = any_of("potencia_lampada_atual"),
    tipo_de_lampada_anterior = any_of("tipo_lampada_anterior"),
    potencia_da_lampada_anterior = any_of("potencia_lampada_anterior"),
    quantidade_ultima_modernizacao = any_of("quantidade_ultima_mod"),
    quantidade_anterior = any_of("quantidade_anterior"),
    tipo_anterior = any_of("tipo_anterior"),
    desc_item_anterior = any_of("desc_item_anterior"),
    cod_item_anterior = any_of("cod_item_anterior"),
    tipo_atual = any_of("tipo_atual"),
    desc_item_atual = any_of("desc_item_atual"),
    cod_item_atual = any_of("cod_item_atual")                                                
    ) %>% 
    mutate(
      data_mod = as.Date(data_mod,"%d/%m/%Y"),
      #hora = as.character(lubridate::hms(hora)),
      data_hora = case_when(
        hora <= "06:00:00" ~ data_mod-1,
        TRUE ~ data_mod
      ),
      mes = month(data_hora),
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
      mes = factor(mes,levels = c("Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro")),
      lat = as.numeric(str_replace(lat,",",".")),
      lon = as.numeric(str_replace(lon,",",".")),
      n_old = coalesce(as.numeric(quantidade_anterior),0),
      n_new = as.numeric(quantidade_ultima_modernizacao)) %>% 
    filter(!is.na(potencia_da_lampada_ultima_modernizacao)) %>% 
    mutate(
      pot_old = sapply(str_split(potencia_da_lampada_anterior,";"), function(x) sum(as.numeric(x),na.rm=T)),
      pot_new =sapply(str_split(potencia_da_lampada_ultima_modernizacao,";"), function(x) sum(as.numeric(x),na.rm=T)),
      eficient = ifelse(pot_old == 0,-1,round(1-(pot_new/pot_old),1))) 
  
  
  arrow::write_parquet(mod, "tt_mod_materiais_rib.parquet")
  
  put_object(
    file = "tt_mod_materiais_rib.parquet",
    object = "tt_mod_materiais_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

mod_rib_extrai_json_api(nome = "Modernizados",
                    raiz_1 = "PONTOS_MODERNIZACAO",
                    raiz_2 = "PONTO_MODERNIZACAO",
                    url = "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPontosModernizacaoCompleto.json?CMD_IDS_PARQUE_SERVICO=1&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print('  PONTOS MODERNIZADOS   - Ok')                

# ----

# OBRAS ----
obras_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
    
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  #dados <- fromJSON(content( GET('https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarObras.json?CMD_OBRAS_ATRASADAS=0&CMD_ID_PARQUE_SERVICO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8', add_headers(`Accept-Encoding` = "gzip")), "text")) %>% 
  #  .[["RAIZ"]] %>%
  #  .[['OBRAS']] %>%
  #  .[['OBRA']]
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  obras <- dados %>% 
    clean_names() %>% 
    select(id_projeto =  num_gco,id_obra,status = status_desc_status, bairro = nome_bairro,rua = desc_obra) %>% 
    mutate(rua  = str_trim(str_replace(rua, "(?i)modernização", ""))) %>% 
    select(id_projeto,rua,bairro,status,id_obra)
  
  
  
  arrow::write_parquet(obras, "tt_obras_rib.parquet")
  
  put_object(
    file = "tt_obras_rib.parquet",
    object = "tt_obras_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

obras_rib_extrai_json_api(nome = "Obras",
                      raiz_1 = "OBRAS",
                      raiz_2 = "OBRA",
                      url = "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarObras.json?CMD_OBRAS_ATRASADAS=0&CMD_ID_PARQUE_SERVICO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8"
)
print('  Obras - Ok')     
# ----
