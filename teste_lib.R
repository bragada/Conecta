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




sol_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
    
    
    corpo_requisicao <- list(
        CMD_ID_STATUS_SOLICITACAO = -1,
        CMD_IDS_PARQUE_SERVICO = "1,2",
        CMD_DATA_RECLAMACAO = "01/03/2023",
        CMD_APENAS_EM_ABERTO = 0
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
    

 
    solicitacoes <- dados %>% 
        clean_names() %>%
        select(protocolo = numero_protocolo,
               data_reclamacao,
               status = desc_status_solicitacao,
               tempo_restante = desc_prazo_restante,
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
                   mes == 12 ~ "Dezembro"
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
print('Ocorrencias/Solicitacoes - Ok')   
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


# Painel Monitoramento ----
p_moni_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

  corpo_requisicao <- list(
    CMD_ID_PARQUE_SERVICO = 2
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
  
  

  
  p_moni <- dados %>% 
    clean_names() %>%
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
      bairro = "Sem Informação",
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
  
  
  
  arrow::write_parquet(p_moni, "tt_painel_monitoramento.parquet")
  
  put_object(
    file = "tt_painel_monitoramento.parquet",
    object = "tt_painel_monitoramento.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

p_moni_extrai_json_api(nome = "Painel de Monitoramento",
                       raiz_1 = "PONTOS_SERVICO",
                       raiz_2 = "PONTO_SERVICO",
                       url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPontosServicoOcorrenciaAndamentoEquipe.json?CMD_ID_PARQUE_SERVICO=2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print(' Painel Monitoramento - Ok')

# ----

# Ordens de Serviço ----
os_extrai_json_api <- function(nome,url,raiz_1,raiz_2){


corpo_requisicao <- list(
  CMD_ID_STATUS_ORDEM_SERVICO = -1,
  CMD_DATA = "01/01/2021",
  CMD_ID_PARQUE_SERVICO = 2
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
  
  os <- dados %>% 
    clean_names() %>%
    select(
      id_ordem_servico,
      data,
      prazo = prazo_restante,
      status = desc_status_ordem_servico,
      desc_tipo_ordem_servico,
      equipe = desc_equipe,
      desc_ordem_servico,
      data_hora_recebido,
      total_pontos,
      total_atendidos
    ) %>% 
    mutate(data= as.POSIXct(strptime(data,"%d/%m/%Y %H:%M")),
           prazo = as.numeric(str_replace(prazo,",",".")),
           tarefas_finalizadas = paste0(total_atendidos,"/",total_pontos),
           avanco = round(100*(total_atendidos/total_pontos),0)
    )
  
  
  
  arrow::write_parquet(os, "tt_ordens_servico.parquet")
  
  put_object(
    file = "tt_ordens_servico.parquet",
    object = "tt_ordens_servico.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

os_extrai_json_api(nome = "Ordens de Serviço",
                   raiz_1 = "ORDENS_SERVICO",
                   raiz_2 = "ORDEM_SERVICO",
                   url="https://conectacampinas.exati.com.br/guia/command/conectacampinas/Ordensdeservico.json?CMD_ID_STATUS_ORDEM_SERVICO=-1&CMD_DATA=01/01/2021&CMD_ID_PARQUE_SERVICO=2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print(' Ordens de Serviço - Ok')

# ----


# Ocorrências Autorizar ----
oa_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
  corpo_requisicao <- list(
   CMD_IDS_PARQUE_SERVICO = 2,
   CMD_PAINEL_NOVO = 1
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
  
  oa <- dados %>% 
    clean_names() %>%
    select(
      protocolo = numero_protocolo,
      tipo_de_ocorrencia = desc_tipo_ocorrencia,
      data_limite_de_atendimento_original = data_limite_atendimento,
      bairro = nome_bairro,
      endereco  = nome_logradouro_completo,
      tempo_paralisado = tempo_pendente,
      data_reclamacao 
    ) %>% 
    mutate(data_limite_de_atendimento_original = as.Date(data_limite_de_atendimento_original,"%d/%m/%Y"),
           data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"))
  
  
  arrow::write_parquet(oa, "tt_ocorrencias_autorizar.parquet")
  
  put_object(
    file = "tt_ocorrencias_autorizar.parquet",
    object = "tt_ocorrencias_autorizar.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

oa_extrai_json_api(nome = "Ocorrências Autorizar",
                   raiz_1 = "PONTOS_SERVICO",
                   raiz_2 = "PONTO_SERVICO",
                   url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarOcorrenciasAutorizar.json?CMD_IDS_PARQUE_SERVICO=2&CMD_PAINEL_NOVO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print('  Ocorrências Autorizar  - Ok')                
# ----
# ATENDIMENTO QUANTO AO PRAZO ----
sgi_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
   
    corpo_requisicao <- list(
        CMD_ID_PARQUE_SERVICO = 2,
        CMD_DATA_INICIAL_FILTRO = "01/01/2021",
        CMD_DATA_FINAL_FILTRO = "01/01/2040",
        CMD_ID_SEM_REGIAO = -1,
        CMD_DETALHADO = 1,
        CMD_CONFIRMADOS = 1
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
  

  
  sgi <- dados %>% 
    clean_names() %>% 
    select(
      atendimento = id_atendimento_ps,
      prazo = data_limite_atendimento,
      prazo_hora = hora_limite_atendimento,
      data_atendimento,
      atendimento_hora = hora_atendimento,
      prev_execucao_horas = previsao_execucao,
      status = no_prazo,
      origem_da_ocorrencia = origem_ocorrencia,
      
    ) %>% 
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
     s3read_using(
      FUN = arrow::read_parquet,
      object = "tt_atendimentos.parquet",
      bucket = "automacao-conecta"
    ) %>% 
        select(no_atendimento,equipe,status_at = atendimento) %>% 
        mutate(no_atendimento = as.character(no_atendimento))
      , by = c("atendimento" = "no_atendimento"))
  
  arrow::write_parquet(sgi, "tt_sgi_atendimento_atendimentos_prazo.parquet")
  
  put_object(
    file = "tt_sgi_atendimento_atendimentos_prazo.parquet",
    object = "tt_sgi_atendimento_atendimentos_prazo.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

sgi_extrai_json_api(nome = "ATENDIMENTO QUANTO AO PRAZO",
                    raiz_1 = "ATENDIMENTOS",
                    raiz_2 = "ATENDIMENTO",
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPrazosAtendimento.json?CMD_ID_PARQUE_SERVICO=2&CMD_DATA_INICIAL_FILTRO=01/01/2021&CMD_DATA_FINAL_FILTRO=01/01/2040&CMD_ID_SEM_REGIAO=-1&CMD_DETALHADO=1&CMD_CONFIRMADOS=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print('ATENDIMENTO QUANTO AO PRAZO  - Ok')                

# ----


# PONTOS MODERNIZADOS -----
mod_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  corpo_requisicao <- list(
  CMD_IDS_PARQUE_SERVICO = 2,
  CMD_MODERNIZACAO = 2,
  CMD_TIPO_CALCULO = 0
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
  #dados <- fromJSON(content( GET('https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPontosModernizacaoCompleto.json?CMD_IDS_PARQUE_SERVICO=2&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs', add_headers(`Accept-Encoding` = "gzip"))
  #                           , "text")) %>% 
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
      etiqueta = id_ponto_servico,
      data_mod = data_ultima_mod,                                      
      hora = hora_ultima_mod,                                           
      equipe = equipe_ultima_mod,                              
      endereco,                                   
      lat = latitude,                                      
      lon = longitude,                                       
      potencia_da_lampada_ultima_modernizacao = potencia_lampada_atual,
      tipo_de_lampada_anterior = tipo_lampada_anterior,                
      potencia_da_lampada_anterior = potencia_lampada_anterior,    
      quantidade_ultima_modernizacao = quantidade_ultima_mod,        
      quantidade_anterior,                                            
      tipo_anterior,                                                 
      desc_item_anterior,                                             
      cod_item_anterior,                                              
      tipo_atual,                                                     
      desc_item_atual,                                               
      cod_item_atual                                                  
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
  
  
  arrow::write_parquet(mod, "tt_mod_materiais.parquet")
  
  put_object(
    file = "tt_mod_materiais.parquet",
    object = "tt_mod_materiais.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

mod_extrai_json_api(nome = "Modernizados",
                    raiz_1 = "PONTOS_MODERNIZACAO",
                    raiz_2 = "PONTO_MODERNIZACAO",
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPontosModernizacaoCompleto.json?CMD_IDS_PARQUE_SERVICO=2&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
)
print('  PONTOS MODERNIZADOS   - Ok')                

# ----


# OBRAS ----
obras_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

corpo_requisicao <- list(
  CMD_OBRAS_ATRASADAS = 0,
  CMD_ID_PARQUE_SERVICO = "1,2"
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
  
  obras <- dados %>% 
    clean_names() %>% 
    select(id_projeto =  num_gco,id_obra,status = status_desc_status, bairro = nome_bairro,rua = desc_obra) %>% 
    mutate(rua  = str_trim(str_replace(rua, "(?i)modernização", ""))) %>% 
    select(id_projeto,rua,bairro,status,id_obra)
  
  
  
  arrow::write_parquet(obras, "tt_obras.parquet")
  
  put_object(
    file = "tt_obras.parquet",
    object = "tt_obras.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

obras_extrai_json_api(nome = "Obras",
                      raiz_1 = "OBRAS",
                      raiz_2 = "OBRA",
                      url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarObras.json?CMD_OBRAS_ATRASADAS=0&CMD_ID_PARQUE_SERVICO=1,2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
)
print('  Obras - Ok')     
# ----
