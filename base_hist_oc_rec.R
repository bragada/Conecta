if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
if (!requireNamespace("lubridate", quietly = TRUE)) install.packages("lubridate")
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")

library(dplyr)
library(lubridate)
library(arrow)
library(aws.s3)


base_his_oc_rec_s3 <- function(dados_atuais, s3_object, s3_bucket, ts_execucao) {

  if (is.null(dados_atuais) || nrow(dados_atuais) == 0) {
    print(paste("Nenhuma observação em dados_atuais para o objeto:", s3_object))
    return(invisible(NULL))
  }
  
  # Verifica se o objeto historico ja existe no bucket
  objeto_existe <- aws.s3::object_exists(object = s3_object, bucket = s3_bucket)
  
  # Se o objeto nao existe, e a primeira execucao de todas.
  if (!objeto_existe) {
    print(paste("Objeto", s3_object, "não encontrado. Criando novo histórico..."))
    novas_ocorrencias <- dados_atuais %>%
      mutate(data_consulta = ts_execucao)
    
    # Escreve para um arquivo temporario local e depois faz o upload
    temp_file <- tempfile(fileext = ".parquet")
    arrow::write_parquet(novas_ocorrencias, temp_file)
    aws.s3::put_object(file = temp_file, object = s3_object, bucket = s3_bucket)
    unlink(temp_file) # Limpa o arquivo temporario
    
    print(paste(nrow(novas_ocorrencias), "registros iniciais adicionados a", s3_object))
    return(invisible(NULL))
  }
  
  # Carrega o historico completo do S3
  historico_completo <- s3read_using(
    FUN = arrow::read_parquet,
    object = s3_object,
    bucket = s3_bucket
  )
  
  data_ultima_consulta_hist <- as_date(max(historico_completo$data_consulta))
  data_execucao_atual <- as_date(ts_execucao)
  
  # Compara a data atual com a ultima data registrada no historico
  if (data_execucao_atual > data_ultima_consulta_hist) {
    # PRIMEIRA EXECUCAO DE UM NOVO DIA
    print(paste("Primeira execução do dia", data_execucao_atual, "para o objeto:", s3_object))
    novas_para_adicionar <- dados_atuais %>%
      mutate(data_consulta = ts_execucao)
    historico_final <- bind_rows(historico_completo, novas_para_adicionar)
    
  } else {
    # EXECUCAO SUBSEQUENTE NO MESMO DIA
    print(paste("Execução subsequente no dia", data_execucao_atual, "para o objeto:", s3_object))
    historico_de_hoje <- historico_completo %>%
      filter(as_date(data_consulta) == data_execucao_atual)
    
    historico_para_comparar <- historico_de_hoje %>% select(-data_consulta)
    colunas_para_comparacao <- setdiff(names(dados_atuais), "dif")

    novas_ocorrencias_unicas_hoje <- anti_join(dados_atuais, historico_para_comparar, by = colunas_para_comparacao)
    
    if (nrow(novas_ocorrencias_unicas_hoje) == 0) {
      print("Nenhuma ocorrência nova encontrada nesta execução.")
      return(invisible(NULL))
    }
    
    novas_para_adicionar <- novas_ocorrencias_unicas_hoje %>%
      mutate(data_consulta = ts_execucao)
    historico_final <- bind_rows(historico_completo, novas_para_adicionar)
  }
  
  # Escreve o historico atualizado de volta para o S3
  temp_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(historico_final, temp_file)
  aws.s3::put_object(file = temp_file, object = s3_object, bucket = s3_bucket)
  unlink(temp_file) # Limpa o arquivo temporario
  
  print(paste(nrow(novas_para_adicionar), "novos registros adicionados a", s3_object))
}






# --- SCRIPT PRINCIPAL DE EXECUCAO ---

# 2. Definir variaveis de configuracao do S3
S3_BUCKET <- "automacao-conecta"
S3_MONI_ATUAL_OBJECT <- "tt_painel_monitoramento.parquet"
S3_OC_ATUAL_OBJECT <- "tt_painel_ocorrencias.parquet"
S3_HIST_MONI_OBJECT <- "tt_hist_p_moni.parquet" # Novo nome para o historico
S3_HIST_OC_OBJECT <- "tt_hist_p_oc.parquet"     # Novo nome para o historico

# 3. Leitura das bases atuais do S3 (seu codigo)
print("Lendo bases de dados atuais do S3...")
p_moni <- s3read_using(FUN = arrow::read_parquet,
                       object = S3_MONI_ATUAL_OBJECT,
                       bucket = S3_BUCKET)

p_oc <- s3read_using(FUN = arrow::read_parquet,
                     object = S3_OC_ATUAL_OBJECT,
                     bucket = S3_BUCKET)
print("Leitura concluída.")

# 4. Processamento do historico
timestamp_execucao <- ymd_hms(now("America/Sao_Paulo"))

print("--- Processando histórico para p_moni ---")
base_his_oc_rec_s3(
  dados_atuais = p_moni,
  s3_object = S3_HIST_MONI_OBJECT,
  s3_bucket = S3_BUCKET,
  ts_execucao = timestamp_execucao
)

print("--- Processando histórico para p_oc ---")
base_his_oc_rec_s3(
  dados_atuais = p_oc,
  s3_object = S3_HIST_OC_OBJECT,
  s3_bucket = S3_BUCKET,
  ts_execucao = timestamp_execucao
)

print("---Bases Históricos - TRUE ---")
