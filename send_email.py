import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Pegando a chave da variável de ambiente
chave_api = os.environ.get("EMAIL_API")

# Criando a conta do SendGrid
conta_sendgrid = SendGridAPIClient(chave_api)

# Criando o e-mail
email = Mail(
    from_email="hkbragada@gmail.com",
    to_emails="rikibragada@gmail.com",
    subject="Fala dodóizin",
    html_content="<p>Ó o cuzin</p>"
)

# Enviando o e-mail
response = conta_sendgrid.send(email)
