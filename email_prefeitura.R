#if (!requireNamespace("emayili", quietly = TRUE)) install.packages("emayili")
#if (!requireNamespace("keyring", quietly = TRUE)) install.packages("keyring")
if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
#if (!requireNamespace("gmailr", quietly = TRUE)) install.packages("gmailr")
#if (!requireNamespace("gargle", quietly = TRUE)) install.packages("gargle")
if (!requireNamespace("emayili", quietly = TRUE)) install.packages("emayili")
if (!requireNamespace("curl", quietly = TRUE)) install.packages("curl")
if (!requireNamespace("googledrive", quietly = TRUE)) install.packages("googledrive")

#if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")

library(googledrive)
#if (!requireNamespace("curl", quietly = TRUE)) install.packages("curl")
#library(curl)
auth_json_path <- tempfile(fileext = ".json")
writeLines(Sys.getenv("GDRIVE_AUTH"), auth_json_path)

# Autentica o googledrive com a conta de serviço
googledrive::drive_auth(path = auth_json_path)

#library(keyring)
library(tidyverse)
library(rmarkdown)

#library(blastula)
#library(emayili)
#Sys.getenv("GMAIL_AUTH")
#packageVersion("emayili")  # Verifique se é a versão mais recente

rmarkdown::pandoc_version()
#rmarkdown::pandoc_version()
render_relatorio <- rmarkdown::render(
 input = "script_relatorio.Rmd",
 output_file = "program_conecta_campinas.pdf"
)

put_object(
    file = "program_conecta_campinas.pdf",
    object = "program_conecta_campinas.pdf",
    bucket = "automacao-conecta",
    region = "sa-east-1"
  )

info_relatorio <- file.info("program_conecta_campinas.pdf")

if(as.Date(info_relatorio$mtime,tz = "America/Sao_Paulo") == Sys.Date()){
    print("s")
  }else {
print("n")}

arquivo_local <- "program_conecta_campinas.pdf"

nome_no_drive <- "program_conecta_campinas.pdf"

# Envie para o Google Drive (pasta raiz). Para pasta específica, veja abaixo.
 drive_upload(media = arquivo_local, name = nome_no_drive, path = as_id('1cdpU2bTo9b29IEVTRsjpqDVciXpsxrYT'), overwrite = TRUE)
#smtp <- server(
#  host = "smtp.gmail.com",
#  port = 587,
#  username = Sys.getenv("SMTP_USER"),
#  password = Sys.getenv("SMTP_PASS"),
#  use_tls = TRUE  # Habilita SSL
#)

# Criar o email e anexar o arquivo temporário
#email <- envelope() %>%
 # from("hkbragada@gmail.com") %>%
 # to("rikibragada@gmail.com") %>%
 # subject("Programação Conecta - Prefeitura") %>%
 # text("Bom dia,  

#  Segue em anexo a programação diária das manutenções e modernizações previstas para a cidade de Campinas.  

#  Bot - HK CONSULTORIA") %>%
#  attachment(path = "program_conecta_campinas.pdf")
#Sys.sleep(5)  # Espera 5 segundos antes do envio

# Enviar o email
#smtp(email, verbose = TRUE)


#} else {

#}
