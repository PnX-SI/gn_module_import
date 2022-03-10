from flask import current_app, render_template
from weasyprint import HTML


def generate_pdf_from_template(template, data, filename):
    template_rendered = render_template(template, data=data)
    html_file = HTML(
        string=template_rendered,
        base_url=current_app.config["API_ENDPOINT"],
        encoding="utf-8",
    )
    return html_file.write_pdf()
