#if (!requireNamespace("emayili", quietly = TRUE)) install.packages("emayili")
#if (!requireNamespace("keyring", quietly = TRUE)) install.packages("keyring")
if (!requireNamespace("rmarkdown", quietly = TRUE)) install.packages("rmarkdown")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
#if (!requireNamespace("gmailr", quietly = TRUE)) install.packages("gmailr")
#if (!requireNamespace("gargle", quietly = TRUE)) install.packages("gargle")
if (!requireNamespace("base64enc", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("tidyverse")
if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("tidyverse")




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

# Caminho do arquivo PDF
arquivo_pdf <- "program_conecta_campinas.pdf"

pdf_base64 <- base64encode(arquivo_pdf)
api_key <- Sys.getenv("API_RESEND")
destinatario <- c(Sys.getenv("EMAIL_USER"))


res <- POST(
    url = "https://api.resend.com/emails",
    add_headers(
        Authorization = paste("Bearer", api_key),
        "Content-Type" = "application/json"
    ),
    body = toJSON(list(
        from = "consultoria@hkbragada.com",
        to = destinatario,
        subject = "Teste de envio com PDF",
        html = "
<p>Bom dia,</p>
<p>Segue em anexo a programação diária das manutenções previstas para a cidade de Campinas.</p>
<p>Atenciosamente,<br>
Agente de IA - HK CONSULTORIA</p>
",
        attachments = list(
            list(
                filename = "program_conecta_campinas.pdf",
                content = pdf_base64
            )
        )
    ), auto_unbox = TRUE)
)

# Conferir resposta
content(res, "parsed")


#put_object(
#    file = "program_conecta_campinas.pdf",
#    object = "program_conecta_campinas.pdf",
#    bucket = "automacao-conecta",
#    region = "sa-east-1"
#  )

#info_relatorio <- file.info("program_conecta_campinas.pdf")

#if(as.Date(info_relatorio$mtime,tz = "America/Sao_Paulo") == Sys.Date()){
#    print("s")
#  }else {
#print("n")}




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
