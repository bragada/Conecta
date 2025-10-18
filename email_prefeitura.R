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
library(base64enc)
library(httr)
library(jsonlite)


# --- Renderizar o PDF ---
render_relatorio <- rmarkdown::render(
  input = "script_relatorio_new.Rmd",
  output_file = "program_conecta_campinas.pdf"
)

# --- Caminho do PDF ---
arquivo_pdf <- "program_conecta_campinas.pdf"
pdf_base64 <- base64encode(arquivo_pdf)

# --- Configurações Resend ---
api_key <- Sys.getenv("API_RESEND")
remetente <- "consultoria@hkbragada.com"

# --- Destinatários (uma única secret, separados por vírgula) ---
destinatarios <- str_split(Sys.getenv("EMAIL_USER"), pattern = ",", simplify = TRUE) %>% as.vector()
print(destinatarios)
# --- Corpo HTML do email ---
html_corpo <- "
<p>Bom dia,</p>
<p>Segue em anexo a programação diária das manutenções previstas para a cidade de Campinas.</p>
<p>Atenciosamente,<br>
Agente de IA - HK CONSULTORIA</p>
"

# --- Função para enviar email ---
enviar_email <- function(destinatario) {
  res <- POST(
    url = "https://api.resend.com/emails",
    add_headers(
      Authorization = paste("Bearer", api_key),
      "Content-Type" = "application/json"
    ),
    body = toJSON(list(
      from = remetente,
      to = destinatario,
      subject = "Programação diária das manutenções",
      html = html_corpo,
      attachments = list(
        list(
          filename = "program_conecta_campinas.pdf",
          content = pdf_base64
        )
      )
    ), auto_unbox = TRUE)
  )
  
  # Retorna status
  list(
    destinatario = destinatario,
    status = content(res, "parsed")
  )
}

# --- Enviar para todos os destinatários ---
resultados <- lapply(destinatarios, enviar_email)

# --- Printar resultados ---
print(resultados)
