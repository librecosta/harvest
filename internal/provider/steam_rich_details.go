package provider

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"slices"
	"strings"

	"github.com/gofurry/steam-go/web/storefront"
)

const (
	steamRichSegmentBytes = 384 << 10
	steamRichDataBytes    = steamRichSegmentBytes - 1024
)

type steamRichRequirements struct {
	Minimum     *string `json:"minimum,omitzero"`
	Recommended *string `json:"recommended,omitzero"`
}

type steamRichSupport struct {
	URL   *string `json:"url,omitzero"`
	Email *string `json:"email,omitzero"`
}

type steamRichDescriptor struct {
	IDs   []int   `json:"ids"`
	Notes *string `json:"notes,omitzero"`
}

type steamRichRating struct {
	Rating      *string `json:"rating,omitzero"`
	RequiredAge *string `json:"required_age,omitzero"`
}

type steamRichDetails struct {
	steamSource
	DetailedDescription *string                      `json:"detailed_description,omitzero"`
	AboutTheGame        *string                      `json:"about_the_game,omitzero"`
	ShortDescription    *string                      `json:"short_description,omitzero"`
	Website             *string                      `json:"website,omitzero"`
	Support             *steamRichSupport            `json:"support_info,omitzero"`
	PC                  *steamRichRequirements       `json:"pc_requirements,omitzero"`
	Mac                 *steamRichRequirements       `json:"mac_requirements,omitzero"`
	Linux               *steamRichRequirements       `json:"linux_requirements,omitzero"`
	Descriptors         *steamRichDescriptor         `json:"content_descriptors,omitzero"`
	Ratings             map[string]steamRichRating   `json:"ratings,omitzero"`
	RequiredAge         *int                         `json:"required_age,omitzero"`
	IsFree              *bool                        `json:"is_free,omitzero"`
	Platforms           *steamPlatforms              `json:"platforms,omitzero"`
	ControllerSupport   *string                      `json:"controller_support,omitzero"`
	Release             *steamRelease                `json:"release,omitzero"`
	SupportedLanguages  *string                      `json:"supported_languages,omitzero"`
	Languages           []storefront.LanguageSupport `json:"languages,omitzero"`
	Truncated           []string                     `json:"truncated,omitzero"`
	Invalid             []string                     `json:"invalid,omitzero"`
}

type steamRichField struct {
	name    string
	budget  int
	value   any
	present func() bool
}

func richField[T any](name string, budget int, v **T) steamRichField {
	return steamRichField{name, budget, v, func() bool { return *v != nil }}
}

func (v *steamRichDetails) fields() []steamRichField {
	return []steamRichField{
		richField("website", 2048, &v.Website), richField("support_info", 3072, &v.Support),
		richField("controller_support", 256, &v.ControllerSupport), richField("platforms", 128, &v.Platforms),
		richField("is_free", 8, &v.IsFree), richField("release_date", 1024, &v.Release),
		richField("content_descriptors", 8192, &v.Descriptors),
		{"ratings", 8192, &v.Ratings, func() bool { return v.Ratings != nil }},
		richField("required_age", 16, &v.RequiredAge),
		richField("pc_requirements", 20<<10, &v.PC), richField("mac_requirements", 20<<10, &v.Mac), richField("linux_requirements", 20<<10, &v.Linux),
		richField("supported_languages", 16<<10, &v.SupportedLanguages),
		richField("short_description", 16<<10, &v.ShortDescription),
		richField("detailed_description", 192<<10, &v.DetailedDescription), richField("about_the_game", 192<<10, &v.AboutTheGame),
	}
}

func richSourceFields(raw []byte, responseKey string) (map[string]jsontext.Value, error) {
	var envelope map[string]struct {
		Data map[string]jsontext.Value `json:"data"`
	}
	err := json.Unmarshal(raw, &envelope)
	return envelope[responseKey].Data, err
}

func decodeSteamRichField(name string, raw jsontext.Value, value any) error {
	switch name {
	case "required_age":
		if string(raw) == `""` {
			return nil
		}
		var age storefront.FlexibleInt
		if err := json.Unmarshal(raw, &age); err != nil {
			return err
		}
		n := age.Int()
		if n < 0 || n > 150 {
			return errors.New("invalid Steam required age")
		}
		*value.(**int) = &n
	case "release_date":
		var date struct {
			ComingSoon *bool  `json:"coming_soon"`
			Date       string `json:"date"`
		}
		if err := json.Unmarshal(raw, &date); err != nil {
			return err
		}
		*value.(**steamRelease) = &steamRelease{ComingSoon: date.ComingSoon, Date: date.Date}
	default:
		if strings.HasSuffix(name, "_requirements") && (string(raw) == "[]" || string(raw) == `""`) {
			*value.(**steamRichRequirements) = &steamRichRequirements{}
			return nil
		}
		return json.Unmarshal(raw, value)
	}
	return nil
}

// Optional malformed detail fields must not invalidate the shared identity or media.
func omitMalformedRichFields(raw []byte) ([]byte, error) {
	var envelope map[string]struct {
		Success bool                      `json:"success"`
		Data    map[string]jsontext.Value `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	for _, row := range envelope {
		v := steamRichDetails{}
		for _, field := range v.fields() {
			value := row.Data[field.name]
			if len(value) != 0 && decodeSteamRichField(field.name, value, field.value) != nil {
				delete(row.Data, field.name)
			}
		}
	}
	return json.Marshal(envelope)
}

func steamRichSegment(t Task, raw []byte, responseKey string) Segment {
	fields, err := richSourceFields(raw, responseKey)
	if err != nil {
		return steamSegment(t, "rich_details", "source_invalid", nil)
	}
	v := steamRichDetails{steamSource: steamProvenance(t)}
	for _, field := range v.fields() {
		body := fields[field.name]
		if len(body) == 0 || string(body) == "null" {
			continue
		}
		if len(body) > field.budget {
			v.Truncated = append(v.Truncated, field.name)
			continue
		}
		if err := decodeSteamRichField(field.name, body, field.value); err != nil {
			_ = json.Unmarshal([]byte("null"), field.value)
			v.Invalid = append(v.Invalid, field.name)
		}
	}
	if v.SupportedLanguages != nil {
		v.Languages = storefront.ParseSupportedLanguages(*v.SupportedLanguages)
	}
	// Preserve complete fields. Drop lowest priority fields first when the
	// combination (including the language projection) is larger than the budget.
	limits := v.fields()
	for i := len(limits) - 1; i >= 0; i-- {
		encoded, err := json.Marshal(v)
		if err == nil && len(encoded) <= steamRichDataBytes {
			break
		}
		field := limits[i]
		if field.present() {
			_ = json.Unmarshal([]byte("null"), field.value)
			if field.name == "supported_languages" {
				v.Languages = nil
			}
			v.Truncated = append(v.Truncated, field.name)
		}
	}
	slices.Sort(v.Truncated)
	slices.Sort(v.Invalid)
	return steamSegment(t, "rich_details", "", v)
}

func normalizeSteamRichDetails(t Task, state string, data jsontext.Value) (jsontext.Value, jsontext.Value, error) {
	v, err := ObservationObject[steamRichDetails](data)
	valid := err == nil && state == "present" && len(data) <= steamRichDataBytes && v.steamSource == steamProvenance(t)
	fields := v.fields()
	for _, field := range fields {
		raw, err := json.Marshal(field.value)
		valid = valid && err == nil && (len(raw) <= field.budget || field.name == "release_date" && len(raw) <= field.budget+8)
	}
	for _, names := range [][]string{v.Truncated, v.Invalid} {
		valid = valid && slices.IsSorted(names)
		for i, name := range names {
			index := slices.IndexFunc(fields, func(f steamRichField) bool { return f.name == name && !f.present() })
			valid = valid && index >= 0 && (i == 0 || names[i-1] != name)
		}
	}
	valid = valid && (v.RequiredAge == nil || *v.RequiredAge >= 0 && *v.RequiredAge <= 150)
	for _, name := range v.Invalid {
		valid = valid && !slices.Contains(v.Truncated, name)
	}
	var languages []storefront.LanguageSupport
	if v.SupportedLanguages != nil {
		languages = storefront.ParseSupportedLanguages(*v.SupportedLanguages)
	}
	expected, _ := json.Marshal(languages)
	actual, _ := json.Marshal(v.Languages)
	return webData(valid && string(expected) == string(actual), v, nil)
}
