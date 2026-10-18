package service

import "testing"

func TestRender_SubstitutesPlaceholders(t *testing.T) {
	got := Render("Horario {{schedule_name}} de la ficha {{ficha}}", map[string]string{
		"schedule_name": "Ene-2026",
		"ficha":         "2850621",
	})
	want := "Horario Ene-2026 de la ficha 2850621"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestRender_UnknownPlaceholderLeftAsIs(t *testing.T) {
	got := Render("Alerta: {{alert_type}} en {{ficha}}", map[string]string{"alert_type": "LOW_ATTENDANCE"})
	want := "Alerta: LOW_ATTENDANCE en {{ficha}}"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestRender_EmptyVars_ReturnsTmplUnchanged(t *testing.T) {
	tmpl := "Sin vars {{placeholder}}"
	if got := Render(tmpl, nil); got != tmpl {
		t.Fatalf("want %q, got %q", tmpl, got)
	}
}

func TestRender_EmptyTemplate(t *testing.T) {
	if got := Render("", map[string]string{"k": "v"}); got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}
