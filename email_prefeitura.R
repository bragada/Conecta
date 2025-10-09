#if (!requireNamespace("emayili", quietly = TRUE)) install.packages("emayili")
#if (!requireNamespace("keyring", quietly = TRUE)) install.packages("keyring")
if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("gmailr", quietly = TRUE)) install.packages("gmailr")
if (!requireNamespace("gargle", quietly = TRUE)) install.packages("gargle")
#if (!requireNamespace("tidyverse", quietly = TRUE)) install.packages("tidyverse")


#if (!requireNamespace("curl", quietly = TRUE)) install.packages("curl")
#library(curl)

#library(keyring)
library(tidyverse)
library(rmarkdown)
library(gmailr)

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


library(gargle)

# Caminho do arquivo PDF a ser enviado
pdf_path <- "program_conecta_campinas.pdf" # ajuste para o nome correto


options(
  gargle_oauth_email = TRUE,
  gargle_oauth_cache = ".secrets"
)

# Autenticando com a service account (arquivo JSON)
gm_auth_configure(path = "sa_gmail.json")
gm_auth(email = "hkbragada@gmail.com")


# Crie o e-mail
email <- gm_mime() %>%
  gm_to("hkbragada@gmail.com") %>%
  gm_from("hkbragada@gmail.com") %>%
  gm_subject("Assunto do e-mail") %>%
  gm_text_body("Segue o PDF em anexo.") %>%
  gm_attach_file(pdf_path, type = "application/pdf")

# Envie o e-mail
gm_send_message(email)

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
