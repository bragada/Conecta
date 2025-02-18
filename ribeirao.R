if (!requireNamespace("base64enc", quietly = TRUE)) install.packages("base64enc")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite")
if (!requireNamespace("janitor", quietly = TRUE)) install.packages("janitor")
if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")


library(base64enc)

library(httr)
library(jsonlite)
library(janitor)
library(tidyverse)
library(aws.s3)
library(arrow)
#credenciais_rib <- paste0("henrique.bragada", ":", "Ihavemacbook13?") %>%
#      base64_enc() %>% 
#      paste("Basic", .)


`%!in%` <- Negate(`%in%`) 
#print(credenciais_rib)

###########################################################################################################

                                                     #RIBEIRAO

 ###########################################################################################################                      
###########################################################################################################                      



credenciais_rib <- paste0(Sys.getenv("USERNAME_RIB"), ":", Sys.getenv("PASSWORD_RIB")) %>%
      base64_enc() %>% 
      paste("Basic", .)


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
  clean_names() %>% 
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


# SOLICITACAOES                       
sol_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
    
    
corpo_requisicao <- list(
  CMD_ID_STATUS_SOLICITACAO="-1",
  CMD_DATA_RECLAMACAO="01/07/2024",
  CMD_IDS_PARQUE_SERVICO="1",
  CMD_APENAS_EM_ABERTO="0"
  
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
  select(  
    protocolo = any_of("numero_protocolo"),
    status = any_of("desc_status_solicitacao"),
    tempo_restante = any_of("desc_prazo_restante"),
    id_ocorrencia = any_of("id_ocorrencia"),
    possui_atendimento_anterior = any_of("possui_atendimento_anterior"),
    endereco_livre_solicitacao = any_of("endereco_livre_solicitacao"),
    origem_ocorrencia = any_of("desc_tipo_origem_solicitacao"),
    pontos = any_of("pontos"),
    data_reclamacao = any_of("data_reclamacao"),
    prazo_restante =  any_of("prazo_restante")
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
      cor_vencimento = case_when(
           prazo_restante <= 0  ~  "darkred",
           prazo_restante > 0 & prazo_restante <= 24 ~  "red",
           prazo_restante > 24 & prazo_restante <= 48 ~  "orange",
           prazo_restante > 48 ~  "olivedrab"
         ),
         semana = week(data_reclamacao) - week(floor_date(data_reclamacao,"month")) +1) 

    
    arrow::write_parquet(solicitacoes, "tt_solicitacoes_rib.parquet")
    
    put_object(
        file = "tt_solicitacoes_rib.parquet",
        object = "tt_solicitacoes_rib.parquet",
        bucket = "automacao-conecta",
        region = 'sa-east-1'
    )
    
}

sol_rib_extrai_json_api(nome = "Solicitações",
                    raiz_1 = "SOLICITACOES",
                    raiz_2 = "SOLICITACAO",
                    url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/Solicitacoes.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=1&CMD_DATA_RECLAMACAO=01/07/2024&CMD_APENAS_EM_ABERTO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8"
) 
print('Solicitações Rib - Ok')
# ----


# Painel Ocorrências ----
p_oc_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
corpo_requisicao <- list(
  CMD_DENTRO_DE_AREA="-1",
  CMD_IDS_PARQUE_SERVICO="1"

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
  
  #osp <- s3read_using(FUN = arrow::read_parquet,
  #                    object = "tt_osp.parquet",
  #                    bucket = "automacao-conecta"
  #)
  
  p_oc <- dados %>% 
  clean_names() %>% 
  select(-endereco) %>% 
  select(
    protocolo = any_of('numero_protocolo'),
    data_limite_atendimento = any_of('data_limite_atendimento'),
    hora_limite_atendimento = any_of('hora_limite_atendimento'),
    tipo_de_ocorrencia = any_of('desc_tipo_ocorrencia'),
    endereco = any_of('nome_logradouro_completo'),
    data_reclamacao = any_of('data_reclamacao'),
    prioridade = any_of('sigla_prioridade_ponto_ocorr'),
    lat = any_of('latitude_total'),
    lon = any_of('longitude_total'),
    equipe = any_of('desc_equipe'),
    origem_ocorrencia = any_of('desc_tipo_origem_ocorrencia'),
    possui_atendimento_anterior = any_of('possui_atendimento_anterior'),
    quant_solicitacoes_vinculadas = any_of('quant_solicitacoes_vinculadas')
  ) %>% 
  left_join(s3read_using(
    FUN = arrow::read_parquet,
    object = "tt_solicitacoes_rib.parquet",
    bucket = "automacao-conecta"
  )   %>% select(protocolo,status,cor_vencimento),by="protocolo") %>% 
  mutate(
    data_limite = paste(data_limite_atendimento,hora_limite_atendimento),
    #recebida =  as.POSIXct(strptime(recebida,"%d/%m/%Y %H:%M")),
    data_limite =as.POSIXct(strptime(data_limite,"%d/%m/%Y %H:%M")),
    prazo_restante = as.numeric(round(difftime(data_limite, as.POSIXct(Sys.time(),"GMT"),units = "hours"),0)),
    data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
    data_limite_atendimento = as.Date(data_limite_atendimento,"%d/%m/%Y"),
    #dias_prazo = as.numeric(data_limite_atendimento - Sys.Date()),
    atrasado = ifelse(prazo_restante < 0, "Atrasada","No Prazo"),
    lat=as.numeric(str_replace(lat,",",".")),
    lon=as.numeric(str_replace(lon,",","."))
  ) %>% 
  mutate(
    cor_atraso = case_when(
      prazo_restante >= 0 ~ "darkgreen",
      TRUE ~ "red"
    )) #%>% 
    #left_join(
    #  osp,by = c("protocolo","id_ocorrencia")
    #) %>% 
    #select(-tipo_de_ocorrencia) %>% 
    #rename(tipo_de_ocorrencia = tipo_ocorrencia)
  
  
  
  arrow::write_parquet(p_oc, "tt_painel_ocorrencias_rib.parquet")
  
  put_object(
    file = "tt_painel_ocorrencias_rib.parquet",
    object = "tt_painel_ocorrencias_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

p_oc_rib_extrai_json_api(nome = "Painel de Ocorrências",
                     raiz_1 = "PONTOS_SERVICO",
                     raiz_2 = "PONTO_SERVICO",
                     url= "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/PaineldeOcorrencias.json?CMD_IDS_PARQUE_SERVICO=1&CMD_DENTRO_DE_AREA=-1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print(' Painel Ocorrências Rib- Ok')

# ----


                       # Painel Monitoramento ----
p_moni_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

corpo_requisicao <- list(

  CMD_IDS_PARQUE_SERVICO="1"

  
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
  select(-endereco) %>% 
  select(
    protocolo = any_of('numero_protocolo'),
    data_limite_atendimento = any_of('data_limite_atendimento'),
    hora_limite_atendimento = any_of('hora_limite_atendimento'),
    tipo_de_ocorrencia = any_of('desc_tipo_ocorrencia'),
    endereco = any_of('nome_logradouro_completo'),
    data_reclamacao = any_of('data_reclamacao'),
    prioridade = any_of('sigla_prioridade_ponto_ocorr'),
    lat = any_of('latitude_total'),
    lon = any_of('longitude_total'),
    equipe = any_of('desc_equipe')
  ) %>% 
  mutate(
    data_limite = paste(data_limite_atendimento,hora_limite_atendimento),
    #recebida =  as.POSIXct(strptime(recebida,"%d/%m/%Y %H:%M")),
    data_limite =as.POSIXct(strptime(data_limite,"%d/%m/%Y %H:%M")),
    prazo_restante = as.numeric(round(difftime(data_limite, as.POSIXct(Sys.time(),"GMT"),units = "hours"),0)),
    data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
    data_limite_atendimento = as.Date(data_limite_atendimento,"%d/%m/%Y"),
    #dias_prazo = as.numeric(data_limite_atendimento - Sys.Date()),
    atrasado = ifelse(prazo_restante < 0, "Atrasada","No Prazo"),
    lat=as.numeric(str_replace(lat,",",".")),
    lon=as.numeric(str_replace(lon,",","."))
  ) %>% 
  mutate(
    cor_atraso = case_when(
      prazo_restante >= 0 ~ "darkgreen",
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
print(' Painel Monitoramento Rib- Ok')

# ----

# ATENDIMENTO QUANTO AO PRAZO ----
sgi_rib_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
   
corpo_requisicao <- list(
  CMD_DATA_FINAL_FILTRO = "01/01/2040",
  CMD_DATA_INICIAL_FILTRO="01/01/2021",
  CMD_IDS_PARQUE_SERVICO="1",
  CMD_ID_SEM_REGIAO = "-1",
  CMD_DETALHADO = '1',
  CMD_CONFIRMADOS='1'
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
  rename(atendimento = id_atendimento_ps,
         prazo = data_limite_atendimento,
         atendimento_hora = hora_atendimento,
         prev_execucao_horas = previsao_execucao,
         status = no_prazo,
         origem_da_ocorrencia = origem_ocorrencia
         ) %>% 
  select(atendimento,
         prazo,
         data_atendimento,
         atendimento_hora,
         prev_execucao_horas,
         status,
         origem_da_ocorrencia
         ) %>% 
  mutate(
    data_atendimento = as.Date(data_atendimento,"%d/%m/%Y"),
    prazo = as.Date(prazo,"%d/%m/%Y"),
    data_atendimento = as.Date(data_atendimento,"%d/%m/%Y"),
    hora = hms(atendimento_hora),
    data_hora = case_when(
      hora <= hms("06:00:00") ~ data_atendimento-1,
      TRUE ~ data_atendimento
    ),
    atendimento = as.character(atendimento) 
  ) %>% 
    left_join(
      s3read_using(
        FUN = arrow::read_parquet,
        object = "tt_atendimentos_rib.parquet",
        bucket = "automacao-conecta"
      )%>% 
        select(no_atendimento,equipe,status_at = atendimento) %>% 
        mutate(no_atendimento = as.character(no_atendimento))
      , by = c("atendimento" = "no_atendimento"))
    
  
  arrow::write_parquet(sgi, "tt_sgi_atendimento_atendimentos_prazo_rib.parquet")
  
  put_object(
    file = "tt_sgi_atendimento_atendimentos_prazo_rib.parquet",
    object = "tt_sgi_atendimento_atendimentos_prazo_rib.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

sgi_rib_extrai_json_api(nome = "ATENDIMENTO QUANTO AO PRAZO",
                    raiz_1 = "ATENDIMENTOS",
                    raiz_2 = "ATENDIMENTO",
                    url = "https://conectaribeiraopreto.exati.com.br/guia/command/conectaribeiraopreto/ConsultarPrazosAtendimento.json?CMD_ID_PARQUE_SERVICO=1&CMD_DATA_INICIAL_FILTRO=01/01/2021&CMD_DATA_FINAL_FILTRO=01/01/2040&CMD_ID_SEM_REGIAO=-1&CMD_DETALHADO=1&CMD_CONFIRMADOS=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjQzIiwiaWF0IjoxNzI5NjM5MjcxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.P1X55Bd9nD9ZxW__ocjTTrGW3qOX68b6CoxiUuKbrz8")
print('ATENDIMENTO QUANTO AO PRAZO Rib - Ok')                

# ----
