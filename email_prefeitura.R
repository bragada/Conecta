if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("gmailr", quietly = TRUE)) install.packages("gmailr")
if (!requireNamespace("gargle", quietly = TRUE)) install.packages("gargle")
if (!requireNamespace("googlesheets4", quietly = TRUE)) install.packages("googlesheets4")

library(tidyverse)
library(rmarkdown)
library(aws.s3)
library(gmailr)
library(gargle)
library(googlesheets4)

# Autenticação Google Sheets (service account)
gs4_auth(path = "sa.json")

# Autenticação Gmailr (OAuth Desktop)
gm_auth_configure(path = "client_secret_gmail.json")
gm_auth(cache = ".gmailr_token")

# Renderizar o relatório
render_relatorio <- rmarkdown::render(
  input = "script_relatorio.Rmd",
  output_file = "program_conecta_campinas.pdf"
)

# Upload para S3
put_object(
  file = "program_conecta_campinas.pdf",
  object = "program_conecta_campinas.pdf",
  bucket = "automacao-conecta",
  region = "sa-east-1"
)

# Verificar se o arquivo foi gerado hoje
info_relatorio <- file.info("program_conecta_campinas.pdf")

if (as.Date(info_relatorio$mtime, tz = "America/Sao_Paulo") == Sys.Date()) {
  print("Relatório gerado hoje - enviando email...")

  # Criar e enviar o email
  email <- gm_mime() %>%
    gm_to("hkbragada@gmail.com") %>%
    gm_from("hkbragada@gmail.com") %>%
    gm_subject("Programação Conecta - Prefeitura") %>%
    gm_text_body("Bom dia,

Segue em anexo a programação diária das manutenções e modernizações previstas para a cidade de Campinas.

Bot - HK CONSULTORIA") %>%
    gm_attach_file("program_conecta_campinas.pdf")

  # Enviar o email
  gm_send_message(email)

  print("Email enviado com sucesso!")

} else {
  print("Relatório não foi gerado hoje")
}
