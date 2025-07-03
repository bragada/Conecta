if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("blastula", quietly = TRUE)) install.packages("blastula")
if (!requireNamespace("gargle", quietly = TRUE)) install.packages("gargle")
if (!requireNamespace("googlesheets4", quietly = TRUE)) install.packages("googlesheets4")
if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")

library(tidyverse)
library(rmarkdown)
library(aws.s3)
library(blastula)
library(gargle)
library(googlesheets4)

# Autenticação Google Sheets (service account)
gs4_auth(path = "sa.json")

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

  # Criar o e-mail
  email <- compose_email(
    body = md("Bom dia,

Segue em anexo a programação diária das manutenções e modernizações previstas para a cidade de Campinas.

Bot - HK CONSULTORIA")
  )

  # Enviar o e-mail usando SMTP do Gmail
  smtp_send(
    email,
    from = Sys.getenv("SMTP_USER"),
    to = "hkbragada@gmail.com",
    subject = "Programação Conecta - Prefeitura",
    attachments = "program_conecta_campinas.pdf",
    credentials = creds_user_pass(
      user = Sys.getenv("SMTP_USER"),
      password = Sys.getenv("SMTP_PASS"),
      provider = "gmail"
    )
  )

  print("Email enviado com sucesso!")

} else {
  print("Relatório não foi gerado hoje")
}
