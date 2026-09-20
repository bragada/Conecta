#install.packages(c("httr", "jsonlite", "janitor", "tidyverse", "aws.s3", "arrow"))
# Pacotes vêm da imagem Docker (ghcr.io/bragada/conecta) -> não instalar em runtime.
# install.packages("base64enc")
# install.packages("janitor")
# install.packages("tidyverse")
# install.packages("aws.s3")
# install.packages("arrow")

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

# ---------------------------------------------------------------------------
# Leitura robusta das APIs EXATI (paginacao / fatiamento por data).
# So a LEITURA muda; manipulacao e upload de cada base seguem iguais.
# A API le os params da QUERY STRING; mandamos na query (e no body, ignorado).
# ---------------------------------------------------------------------------
BASE_CAMP <- "https://conectacampinas.exati.com.br/guia/command/conectacampinas/"

# Tenta ate `tent` vezes: consultas grandes devolvem 502/timeout esporadico, e
# uma falha silenciosa aqui vira base incompleta no S3. Devolve NULL se desistir.
.post_exati <- function(path, params, tent = 3L) {
  qs <- paste0(names(params), "=", unlist(params), collapse = "&")
  u  <- paste0(BASE_CAMP, path, "?", qs)
  for (i in seq_len(tent)) {
    r <- tryCatch(POST(u, add_headers(Authorization = credenciais, `Accept-Encoding` = "gzip"),
                       body = params, encode = "json", timeout(300)),
                  error = function(e) e)
    if (!inherits(r, "error") && status_code(r) == 200) return(r)
    if (i < tent) Sys.sleep(5 * i)
  }
  NULL
}
.no_raiz <- function(resp, raiz) {
  node <- fromJSON(content(resp, "text", encoding = "UTF-8"))[["RAIZ"]]
  for (n in raiz) node <- if (is.null(node)) NULL else node[[n]]
  node
}
.para_chr <- function(d) dplyr::mutate(tibble::as_tibble(d), dplyr::across(dplyr::everything(), as.character))

# Pontos modernizados: leitura por JANELA DE DATA (nao por CMD_PAGE).
#
# Por que nao CMD_PAGE: a ordenacao do servidor nao e estavel entre paginas.
# Medido em 19/09/2026 -> pg1 = ids 139798..149532, pg2 = 149533..159203 (ok,
# por id crescente), pg3 = 130095..271717 (embaralha). Dai em diante as paginas
# se sobrepoem, e o distinct() no fim colapsava ~117k linhas baixadas em 68.681
# unicas. Foi assim que tt_mod_materiais caiu de ~116k para 68.681 sem erro.
#
# A janela de data independe da ordenacao: CMD_DATA_INICIO/CMD_DATA_FIM trazem
# os pontos cujo intervalo [primeira mod, ultima mod] intersecta a janela.
# Ladrilhando o calendario mes a mes, todo ponto cai em pelo menos uma janela
# (os poucos com mais de uma modernizacao aparecem em duas -> distinct()).
# Teto de ~9.000 por consulta -> divide a janela ao meio.
#
# Validado em 19/09/2026: 57 chamadas, 0 falhas, 116.762 pontos unicos, e os
# 68.681 que estavam no S3 continuam todos presentes (superconjunto estrito).
# Custo: ~18 min de leitura (antes ~6 min, incompletos).

# tt_mod_materiais e tt_mod_lum fazem exatamente a mesma consulta; sem o cache
# o script pagaria esses ~18 min duas vezes na mesma run.
.cache_mod <- new.env(parent = emptyenv())

ler_modernizados <- function(path, params, raiz, ini = as.Date("2023-01-01"), cap = 9000L) {
  chave <- paste(path, paste(names(params), unlist(params), sep = "=", collapse = "&"),
                 paste(raiz, collapse = "/"), ini, cap, sep = "|")
  if (!is.null(.cache_mod[[chave]])) {
    message("modernizados: reaproveitando leitura ja feita nesta run.")
    return(.cache_mod[[chave]])
  }
  fmt <- function(d) format(d, "%d/%m/%Y")
  acc <- list(); falhou <- FALSE
  jan <- function(a, b) {
    if (falhou) return(invisible())
    r <- .post_exati(path, c(params, CMD_DATA_INICIO = fmt(a), CMD_DATA_FIM = fmt(b)))
    if (is.null(r)) {
      falhou <<- TRUE
      message("EXATI nao respondeu na janela ", fmt(a), "..", fmt(b), " -> leitura abortada (nada sobe).")
      return(invisible())
    }
    d <- .no_raiz(r, raiz); n <- if (is.null(d)) 0L else nrow(as.data.frame(d))
    if (n >= cap && a < b) {
      m <- a + floor(as.numeric(b - a) / 2)
      jan(a, m); jan(m + 1, b); return(invisible())
    }
    if (n >= cap && a == b)
      message("ATENCAO: ", fmt(a), " bateu no teto de ", cap, " num unico dia -> pode faltar dado nesse dia.")
    if (n > 0) acc[[length(acc) + 1]] <<- .para_chr(d)
  }
  bordas <- seq(ini, as.Date(format(Sys.Date(), "%Y-%m-01")), by = "month")
  for (k in seq_along(bordas))
    jan(bordas[k], if (k < length(bordas)) bordas[k + 1] - 1 else Sys.Date())
  if (falhou || !length(acc)) return(NULL)
  out <- dplyr::distinct(dplyr::bind_rows(acc))
  .cache_mod[[chave]] <- out
  out
}

# Sobe pro S3 so se a base nova nao encolheu de forma suspeita frente a que ja
# esta la. O guard antigo (<= 10 linhas) deixou passar semanas de base pela
# metade; queda grande agora cancela o upload e preserva a copia boa.
#
# USAR SO EM BASE CUMULATIVA -- historico que so cresce: atendimentos,
# solicitacoes, ordens de servico, modernizados. Nessas, queda grande e sempre
# defeito de leitura, nunca o dado real.
#
# NAO USAR EM BASE DE SNAPSHOT -- foto do estado atual, como o painel de
# monitoramento (tt_painel_monitoramento): ali encolher e resultado legitimo
# (menos ocorrencia em aberto = menos linha) e o guard barraria atualizacao
# correta. Por isso ele e opt-in: as demais bases seguem com put_object direto.
sobe_s3 <- function(df, objeto, tolerancia = 0.10) {
  ant <- tryCatch(aws.s3::s3read_using(FUN = arrow::read_parquet, object = objeto,
                                       bucket = "automacao-conecta"),
                  error = function(e) NULL)
  if (!is.null(ant) && nrow(df) < nrow(ant) * (1 - tolerancia)) {
    message("ATENCAO: ", objeto, " viria com ", nrow(df), " linhas contra ", nrow(ant),
            " no S3 (queda de ", round(100 * (1 - nrow(df) / nrow(ant))), "%). Upload CANCELADO.")
    return(invisible(FALSE))
  }
  arrow::write_parquet(df, objeto)
  put_object(file = objeto, object = objeto, bucket = "automacao-conecta", region = "sa-east-1")
  message(objeto, ": ", nrow(df), " linhas enviadas.")
  invisible(TRUE)
}

# Fatiamento por data (teto ~10.000/consulta) -> divide [ini,fim] ao meio recursivo.
ler_por_data <- function(path, params, raiz, ini, fim, par_ini, par_fim, cap = 10000L) {
  fmt <- function(d) format(d, "%d/%m/%Y")
  jan <- function(a, b) {
    r <- .post_exati(path, c(params, setNames(list(fmt(a), fmt(b)), c(par_ini, par_fim))))
    d <- if (status_code(r) == 200) .no_raiz(r, raiz) else NULL
    n <- if (is.null(d)) 0L else nrow(as.data.frame(d))
    if (n >= cap && a < b) { m <- a + floor(as.numeric(b - a) / 2); return(dplyr::bind_rows(jan(a, m), jan(m + 1, b))) }
    if (n == 0) return(NULL)
    .para_chr(d)
  }
  out <- jan(as.Date(ini), as.Date(fim))
  if (is.null(out) || !nrow(out)) return(NULL)
  dplyr::distinct(out)
}


# ATENDIMENTOS
at_extrai_json_api <- function(nome,url,raiz_1,raiz_2){


  corpo_requisicao <- list(
        CMD_IDS_PARQUE_SERVICO="2",
        CMD_DATA_INICIO="01/03/2023"
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
    rename(endereco = nome_logradouro_completo,
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
    #filter(atendimento %!in% c("MOD: RETRABALHO", "MOD: Atendido")) %>%
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
    select(id_ponto_servico,no_atendimento, protocolo, tipo_de_ocorrencia, atendimento, motivo, lat, lon, nome_bairro, endereco, data_atendimento, hora_inicio, hora_conclusao, equipe, semana_marco, mes, hora, data_hora, dia_semana, semana) 
  
  
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
                   url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarAtendimentoPontoServico.json?CMD_IDS_PARQUE_SERVICO=2&CMD_DATA_INICIO=01/03/2023&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw"
) 
print('Atendimentos - Ok')



# SOLICITACOES
sol_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
  corpo_requisicao <- list(
  CMD_ID_STATUS_SOLICITACAO = -1,
  CMD_IDS_PARQUE_SERVICO = "2",
  CMD_DATA_RECLAMACAO = "01/03/2024",
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
                    url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/webservice-consultarsolicitacao.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=2&CMD_DATA_RECLAMACAO=01/03/2023"
) 
print('Solicitações - Ok')
# ----


# Ocorrencias/Solicitacoes Pendentes Realizadas ----
osp_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
 
 
corpo_requisicao <- list(
 CMD_ID_PARQUE_SERVICO = "[1,2]",
 CMD_AGRUPAMENTO = "OCORRENCIA_PONTO_SERVICO",
 CMD_STATUS = "TODOS",
 CMD_ORIGEM_ATENDIMENTO = "TODOS",
 CMD_TIPO_SOLICITACAO = "TODOS",
 CMD_DATA_INICIO = "01/03/2023",
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
select(
    data_limite_atendimento_data,
    hora_limite_atendimento,
    data_limite_atendimento,
    endereco,
    nome_bairro,
    status = desc_status_atendimento_ps,
    motivo = desc_motivo_atendimento_ps,
    solucao = desc_solucao_atendimento_ps,
    tipo_ocorrencia = desc_tipo_ocorrencia,
    equipe = desc_equipe,
    id_status_ocorrencia,
    origem_ocorrencia = desc_tipo_origem_ocorrencia,
    data_ordem_servico,
    id_ordem_servico,
    id_atendimento = id_atendimento_ps ,
    data_hora_reclamacao,
    data_reclamacao,
    hora_reclamacao,
    #data_atendimento,
    #hora_conclusao,
    data_hora_conclusao_atendimento,
    desc_prazo,
    protocolo= numero_protocolo
  )
 

 
 arrow::write_parquet(osp, "tt_osp.parquet")

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
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarOcorrenciasSolicitacoesPendentesRealizadas.json?CMD_ID_PARQUE_SERVICO=[1,2]&CMD_AGRUPAMENTO=OCORRENCIA_PONTO_SERVICO&CMD_STATUS=TODOS&CMD_ORIGEM_ATENDIMENTO=TODOS&CMD_TIPO_SOLICITACAO=TODOS&CMD_DATA_INICIO=01/03/2023&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print('Ocorrencias/Solicitacoes - Ok')   
# ----

# BASE EMAIL ----
email_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
 
 
corpo_requisicao <- list(
 CMD_ID_PARQUE_SERVICO = "[1,2]",
 CMD_AGRUPAMENTO = "OCORRENCIA_PONTO_SERVICO",
 CMD_STATUS = "PENDENTES",
 CMD_ORIGEM_ATENDIMENTO = "TODOS",
 CMD_TIPO_SOLICITACAO = "TODOS",
 CMD_DATA_INICIO = "01/03/2023",
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

email <<- dados %>% 
clean_names() %>% 
select(
    data_limite_atendimento_data,
    hora_limite_atendimento,
    data_limite_atendimento,
    endereco,
    nome_bairro,
    tipo_ocorrencia = desc_tipo_ocorrencia,
    equipe = desc_equipe,
    id_status_ocorrencia,
    origem_ocorrencia = desc_tipo_origem_ocorrencia,
    data_ordem_servico,
    id_ordem_servico,
    data_hora_reclamacao,
    data_reclamacao,
    hora_reclamacao,
    desc_prazo,
    protocolo= numero_protocolo
  )
 

 
 arrow::write_parquet(email, "tt_email.parquet")

 put_object(
   file = "tt_email.parquet",
   object = "tt_email.parquet",
   bucket = "automacao-conecta",
   region = 'sa-east-1'
 )
 
}
email_extrai_json_api(nome = "Ocorrencias/Solicitacoes Pendentes Realizadas ",
                    raiz_1 = "OCORRENCIAS_SOLICITACOES",
                    raiz_2 = "OCORRENCIA_SOLICITACAO",
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarOcorrenciasSolicitacoesPendentesRealizadas.json?CMD_ID_PARQUE_SERVICO=[1,2]&CMD_AGRUPAMENTO=OCORRENCIA_PONTO_SERVICO&CMD_STATUS=PENDENTES&CMD_ORIGEM_ATENDIMENTO=TODOS&CMD_TIPO_SOLICITACAO=TODOS&CMD_DATA_INICIO=01/03/2023&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs")
print('Base Email - Ok')   
# ----

# Painel Ocorrências ----
p_oc_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
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

  #dados %>%  glimpse()
      
  
  p_oc <- dados %>% 
    clean_names() %>% 
    select(
      protocolo = numero_protocolo ,
      id_ponto_servico,
      tipo_de_ocorrencia = desc_tipo_ocorrencia,
      origem_ocorrencia = desc_tipo_origem_ocorrencia,
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
      )) #%>% 
    #left_join(
    #  osp,by = c("protocolo","id_ocorrencia")
    #) %>% 
    #select(-tipo_de_ocorrencia) %>% 
    #rename(tipo_de_ocorrencia = tipo_ocorrencia)
  
  
  
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
                     url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/PaineldeOcorrencias.json?CMD_IDS_PARQUE_SERVICO=2&CMD_DENTRO_DE_AREA=-1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw")
print(' Painel Ocorrências - Ok')

# ----


# Painel Monitoramento ----
p_moni_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

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
  
  if (is.null(dados) || length(dados) == 0) {
  print("Objeto 'dados' está nulo ou vazio. Seguindo o fluxo.")
} else {


    p_moni <- dados %>% 
    clean_names() %>%
    select(
      equipe = any_of("desc_equipe"),
      id_ponto_servico = any_of("id_ponto_servico"),
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
      #bairro = "Sem Informação",
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
  

}

p_moni_extrai_json_api(nome = "Painel de Monitoramento",
                       raiz_1 = "PONTOS_SERVICO",
                       raiz_2 = "PONTO_SERVICO",
                       url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPontosServicoOcorrenciaAndamentoEquipe.json?CMD_IDS_PARQUE_SERVICO=2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw")
print(' Painel Monitoramento - Ok')

# ----

# Ordens de Serviço ----
os_extrai_json_api <- function(nome,url,raiz_1,raiz_2){


  dados <- ler_por_data("Ordensdeservico.json",
    list(CMD_IDS_PARQUE_SERVICO = "2", CMD_ID_STATUS_ORDEM_SERVICO = "-1"),
    c(raiz_1, raiz_2),
    ini = "2023-01-01", fim = Sys.Date(),
    par_ini = "CMD_DATA_INICIAL", par_fim = "CMD_DATA_FINAL", cap = 10000L)
  
  
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
      total_atendidos,
      id_obra = ids_obra
    ) %>% 
        mutate(total_pontos = as.numeric(total_pontos),
           total_atendidos = as.numeric(total_atendidos)
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
                   url="https://conectacampinas.exati.com.br/guia/command/conectacampinas/Ordensdeservico.json?CMD_DATA_INICIAL=01/01/2023&CMD_IDS_PARQUE_SERVICO=2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw")
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
                   url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarOcorrenciasAutorizar.json?CMD_ID_PARQUE_SERVICO=2&CMD_PAINEL_NOVO=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzM5Mzg2MDQ4LCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.dngF6qc31is6RSLvSeBjGxcU8GqvoMtGdQqPJTZNDoI")
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
      id_ordem_servico,
      atendimento = id_atendimento_ps,
      prazo = data_limite_atendimento,
      prazo_hora = hora_limite_atendimento,
      data_atendimento = data_atendimento,
      data_reclamacao,
      atendimento_hora = hora_atendimento,
      prev_execucao_horas = previsao_execucao,
      status = no_prazo,
      origem_da_ocorrencia = origem_ocorrencia,
      protocolo = numero_protocolo
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
    #filter(!is.na(hora_atendimento)) %>% 
    left_join(
     s3read_using(
      FUN = arrow::read_parquet,
      object = "tt_atendimentos.parquet",
      bucket = "automacao-conecta"
    ) %>% 
        select(no_atendimento,equipe,status_at = atendimento,motivo,equipe,endereco) %>% 
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
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPrazosAtendimento.json?CMD_IDS_PARQUE_SERVICO=2&CMD_DATA_INICIAL_FILTRO=01/01/2021&CMD_DATA_FINAL_FILTRO=01/01/2040&CMD_ID_SEM_REGIAO=-1&CMD_DETALHADO=1&CMD_CONFIRMADOS=1&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw")
print('ATENDIMENTO QUANTO AO PRAZO  - Ok')                

# ----


# PONTOS MODERNIZADOS -----
mod_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  dados <- ler_modernizados("ConsultarPontosModernizacaoCompleto.json",
    list(CMD_IDS_PARQUE_SERVICO = "2", CMD_MODERNIZACAO = "3"),
    c(raiz_1, raiz_2))
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
    num_gco,
    id_ponto_servico,
    data_mod = data_ultima_mod,                                      
    hora = hora_ultima_mod,                                           
    equipe = equipe_last,                              
    endereco,                                   
    lat = latitude,                                      
    lon = longitude,                                       
    potencia_da_lampada_ultima_modernizacao = potencia_lampada_atual,
    tipo_de_lampada_anterior = tipo_lampada_anterior,                
    potencia_da_lampada_anterior = potencia_lampada_anterior,    
    quantidade_ultima_modernizacao = quantidade_ultima_mod,        
    quantidade_anterior,                                            
    tipo_anterior = tipo_lampada_anterior,                                                 
    desc_item_anterior = desc_itens_last4,                                             
    cod_item_anterior = cod_itens_last4,                                              
    tipo_atual = desc_itens_last5,                                                     
    desc_item_atual = desc_itens_last5,                                               
    cod_item_atual  =   cod_itens_last5   



          
 
#etiqueta = id_ponto_servico,
#data_mod = data_ultima_mod,                                      
#hora = hora_ultima_mod,                                           
#equipe = equipe_ultima_mod,                              
#endereco,                                   
#lat = latitude,                                      
#lon = longitude,                                       
#potencia_da_lampada_ultima_modernizacao = potencia_lampada_atual,
#tipo_de_lampada_anterior = tipo_lampada_anterior,                
#potencia_da_lampada_anterior = potencia_lampada_anterior,    
#quantidade_ultima_modernizacao = quantidade_ultima_mod,        
#quantidade_anterior,                                            
#tipo_anterior,                                                 
#desc_item_anterior,                                             
#cod_item_anterior,                                              
#tipo_atual,                                                     
#desc_item_atual,                                               
#cod_item_atual                                                 
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
    #filter(!is.na(potencia_da_lampada_ultima_modernizacao)) %>% 
    mutate(
      pot_old = sapply(str_split(potencia_da_lampada_anterior,";"), function(x) sum(as.numeric(x),na.rm=T)),
      pot_new =sapply(str_split(potencia_da_lampada_ultima_modernizacao,";"), function(x) sum(as.numeric(x),na.rm=T)),
      eficient = ifelse(pot_old == 0,-1,round(1-(pot_new/pot_old),1))) 
  
  
  sobe_s3(mod, "tt_mod_materiais.parquet")
  
}

mod_extrai_json_api(nome = "Modernizados",
                    raiz_1 = "PONTOS_MODERNIZACAO",
                    raiz_2 = "PONTO_MODERNIZACAO",
                    url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarPontosModernizacaoCompleto.json?CMD_MODERNIZACAO=3&CMD_AGRUPAMENTO=2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTc0MTMwODExNSwib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.kqtz1rMiFi_fYv8sAjQ66wXh3gddF3mCiPIojxz-oZ8"
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
                      url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/ConsultarObras.json?CMD_OBRAS_ATRASADAS=0&CMD_IDS_PARQUE_SERVICO=1,2&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW9yZGFuby5jbGFib25kZUBleGF0aS5jb20uYnIiLCJqdGkiOiIyMTg3IiwiaWF0IjoxNzQxMzA2MjUxLCJvcmlnaW4iOiJHVUlBLVNFUlZJQ0UifQ.4pOO-PcgG-XF8c8L1fDeX2PauVCPNU0OBIcJ3J2WLGw"
)
print('  Obras - Ok')          
                  
# ----

                       # OBRAS ----
mod_lum_extrai_json_api <- function(nome,url,raiz_1,raiz_2){

  dados <- ler_modernizados("ConsultarPontosModernizacaoCompleto.json",
    list(CMD_IDS_PARQUE_SERVICO = "2", CMD_MODERNIZACAO = "3"),
    c(raiz_1, raiz_2))
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  # A API devolve DD/MM/YYYY. Sem o format, as.Date() tenta %Y-%m-%d e %Y/%m/%d e
  # le "31/12/2025" como 0031-12-20 (ano <- dia, dia <- "20" do ano) -- silencioso.
  # Mesmo format que mod_extrai_json_api ja usa sobre este mesmo campo.
  mod_lum <- dados %>% clean_names() %>% mutate(data_mod = as.Date(data_ultima_mod, "%d/%m/%Y"))
  
  
  sobe_s3(mod_lum, "tt_mod_lum.parquet")
  
}

mod_lum_extrai_json_api(nome = "mod_lum",
                      raiz_1 = "PONTOS_MODERNIZACAO",
                      raiz_2 = "PONTO_MODERNIZACAO",
                      url = "https://conectacampinas.exati.com.br/guia/command/conectacampinas/webservice-consultarpontosmodernizacaocompleto.json?CMD_IDS_PARQUE_SERVICO=2&CMD_PAGE_SIZE=0&CMD_MODERNIZACAO=2&CMD_TIPO_CALCULO=1"
)
print('  Mod Lum - Ok')     


###########################################################################################################

                                                     #RIBEIRAO

 ###########################################################################################################                      
###########################################################################################################                      

