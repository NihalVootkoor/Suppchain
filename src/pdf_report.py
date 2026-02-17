"""PDF report generation."""

from __future__ import annotations

import logging
import unicodedata
from io import BytesIO
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _safe_text(value: object) -> str:
    """Coerce text to latin-1 safe string for built-in PDF fonts."""

    text = str(value or "")
    try:
        text.encode("latin-1")
        return text
    except UnicodeEncodeError:
        normalized = unicodedata.normalize("NFKD", text)
        return normalized.encode("latin-1", "ignore").decode("latin-1")


def generate_pdf(
    events: list[dict[str, object]], kpis: Optional[Dict[str, object]] = None
) -> Optional[bytes]:
    """Generate a PDF report for the given events and KPIs."""

    try:
        from fpdf import FPDF  # type: ignore
    except Exception as exc:
        logger.warning("FPDF not available for PDF generation: %s", exc)
        return None

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, "Automotive Supply Chain Risk Report", ln=True)
    if kpis:
        pdf.set_font("Arial", size=10)
        for key, value in kpis.items():
            pdf.cell(0, 6, _safe_text(f"{key}: {value}"), ln=True)
        pdf.ln(2)
    for event in events:
        pdf.set_font("Arial", style="B", size=11)
        pdf.multi_cell(0, 6, _safe_text(event.get("title", "Untitled")))
        pdf.set_font("Arial", size=10)
        pdf.multi_cell(0, 5, _safe_text(event.get("event_summary", "")))
        mitigation = event.get("mitigation_description")
        if mitigation:
            pdf.multi_cell(0, 5, _safe_text(mitigation))
        pdf.ln(2)
    output = BytesIO()
    pdf.output(output)
    return output.getvalue()
