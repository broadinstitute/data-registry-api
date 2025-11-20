"""replace_sgc_phenotypes_data

Revision ID: replace_sgc_phenotypes_data
Revises: add_column_mapping_sgc_files
Create Date: 2025-11-20 14:36:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'replace_sgc_phenotypes_data'
down_revision = 'add_column_mapping_sgc_files'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Clear existing data
    conn.execute(text("DELETE FROM `sgc_phenotypes`"))
    
    # Insert new phenotype data from pheno_names.txt
    phenotypes = [
        ("ANOGENITAL_WARTS", "Anogenital (venereal) warts"),
        ("MELANOMA_ACRAL", "Acral Melanoma"),
        ("HERPESVIRAL_INF", "Herpesviral [herpes simplex] infections"),
        ("ZOSTER", "Zoster [herpes zoster]"),
        ("OTHER_WARTS", "Other viral warts"),
        ("MOLLUSCUM", "Molluscum contagiosum"),
        ("FUNGAL_INF", "Dermatophytosis [fungal infections]"),
        ("SCABIES", "Scabies"),
        ("MELANOMA_MALIGNT", "Malignt melanoma"),
        ("KERATINOCYTE_CANCER", "Keratinocyte cancers"),
        ("BC_CANCER", "Basal cell carcinoma"),
        ("SC_CANCER", "Squamous cell carcinoma"),
        ("MELANOMA_SITU", "Melanoma in situ"),
        ("DRESS", "Drug rash with eosinophilia and systemic symptoms syndrome [DRESS]"),
        ("SARCOIDOSIS", "Sarcoidosis of skin"),
        ("HT_SCAR", "Hypertrophic scar"),
        ("KELOID", "Keloid scar"),
        ("IMPETIGO", "Impetigo"),
        ("CUTANEOUS_ABSCESS", "Cutaneous abscess, furuncle and carbuncle"),
        ("CELLULITIS", "Cellulitis"),
        ("CYTST_PILONIDAL", "Pilonidal cyst"),
        ("CYST_ABSCESS", "Pilonidal cyst with abscess"),
        ("CYST_WITHOUT", "Pilonidal cyst without abscess"),
        ("OTHER_INFECTION", "Other local infections of the skin"),
        ("PEMPHIGUS", "Pemphigus"),
        ("PEMPHIGUS_VUL", "Pemphigusvulgaris"),
        ("PEMPHIGUS_FOL", "Pemphigus foliaceus"),
        ("PEMPHIGOID", "Pemphigoid"),
        ("BUL_PEMPHIGOID", "Bullous Pemphigoid"),
        ("CIC_PEMPHIGOID", "Cicatricial pemphigoid"),
        ("HERPETIFORMIS_DERM", "Dermatitis herpetiformis"),
        ("ATOPIC_DERM", "Atopic dermatitis"),
        ("SEBORRHOEIC_DERM", "Seborrhoeic dermatitis"),
        ("ALLERGIC_DERM", "Allergic contact dermatitis"),
        ("CONTACT_DERM", "Contact dermatitis (allergic, irritant or unspecified)"),
        ("IRRITANT_DERM", "Irritant contact dermatitis"),
        ("SUBSTANCE_DERM", "Dermatitis due to substances taken interlly"),
        ("LSC_PRURIGO", "Lichen simplex chronicus and prurigo"),
        ("LICHEN_SC", "Lichen simplex chronicus"),
        ("PRURIGO_NOD", "Prurigo nodularis"),
        ("PRURITUS", "Pruritus"),
        ("OTHER_DERM", "Other dermatitis"),
        ("PSOR", "Psoriasis"),
        ("PSOR_VULGARIS", "Psoriasis vulgaris"),
        ("PUSTULAR_PSOR", "Generalized pustular psoriasis"),
        ("PALMOPLANTAR_PSOR", "Pustulosis palmaris et plantaris"),
        ("GUTTATE_PSOR", "Guttate psoriasis"),
        ("PSOR_ARTH", "Psoriatic arthritis"),
        ("LICHEN_PLANUS", "Lichen planus"),
        ("URTICARIA", "Urticaria"),
        ("ERYTHEMA_MULTI", "Erythema multiforme"),
        ("SJS_LYELL", "Bullous erythema multiforme [SJS] &Toxic epidermal necrolysis [Lyell]"),
        ("OTHER_ERYTHEMA", "Other erythema multiforme"),
        ("ERYTHEMA_NODOSUM", "Erythema nodosum"),
        ("ERYTHEMA_UNSPEC", "Erythematous condition, unspecified"),
        ("SOLAR_URTICARIA", "Solar urticaria"),
        ("PLE", "Polymorphous light eruption"),
        ("ACTINIC_KER", "Actinic keratosis"),
        ("ALOPECIA", "Cicatricial alopecia [scarring hair loss]"),
        ("HIRSUTISM", "Hirsutism"),
        ("ACNE_VULGARIS", "Acne vulgaris"),
        ("ROSACEA", "Rosacea"),
        ("CYST_FOLLICULAR", "Follicular cyst"),
        ("CYST_EPIDERMAL", "Epidermal cyst"),
        ("CYST_PILAR", "Trichilemmal cysts [pilar cyst]"),
        ("ACNE_KELOID", "Acne keloid "),
        ("HID_SUPP", "Hidradenitis suppurativa"),
        ("ANHIDROSIS", "Anhidrosis"),
        ("BROMHIDROSIS", "Bromhidrosis"),
        ("VITILIGO", "Vitiligo"),
        ("FRECKLES", "Freckles"),
        ("CALS", "Cafe au Lait spots"),
        ("OTHER_HYPERPIG", "Other melanin hyperpigmentation"),
        ("SEBORRHOEIC_KER", "Seborrhoeic keratosis"),
        ("ACANTHOSIS_NIGRICANS", "Acanthosis nigricans"),
        ("CORNS", "Corns and callosities"),
        ("OTHER_THICKENING", "Other epidermal thickening"),
        ("PYODERMA", "Pyoderma gangrenosum"),
        ("DECUBITUS_ULCER", "Decubitus ulcer"),
        ("LICHEN_SA", "Lichen sclerosus et atrophicus"),
        ("SCAR_FIBROSIS", "Scar conditions and fibrosis of skin"),
        ("HTS_KELOID", "Hypertrophic scar or keloid scar"),
        ("OTHER_HT", "Other hypertrophic disorders of the skin"),
        ("SKIN_GRANULOMA", "Granulomatous disorders of skin and subcutaneous tissue"),
        ("GRANULOMA_ANNULARE", "Granuloma annulare"),
        ("LIPOIDICA", "Necrobiosis lipoidica"),
        ("LUPUS", "Lupus erythematosus"),
        ("DISCOID_LUPUS", "Discoid lupus erythematosus"),
        ("CUTANEOUS_LUPUS", "Subacute cutaneous lupus"),
        ("MORPHEA", "Localised (or linear) scleroderma [morphea]"),
        ("CALCINOSIS_CUTIS", "Calcinosis cutis"),
        ("SCLERODACTYLY", "Sclerodactyly"),
        ("GOTTRON", "Gottron papules"),
        ("VASCULITIS", "Vasculitis limited to the skin"),
        ("ULCER", "Ulcer"),
        ("GRANULOMA", "Pyogenic granuloma"),
        ("SWEET", "Febrile neutrophilic dermatosis [Sweet]"),
        ("AMYLOIDOSIS", "Amyloidosis of the skin"),
        ("DERMATOPOLYMYOSITIS", "Dermatopolymyositis"),
        ("GES_PEMPHIGOID", "Herpes gestationis [Pemphigoid gestationis]"),
        ("PUPPP", "Pruritic urticarial papules and plaques of pregncy (PUPPP) [polymorphic or atopic eruption of pregncy]"),
        ("ICHTHYOSIS", "Ichthyosis vulgaris"),
    ]
    
    insert_query = "INSERT INTO `sgc_phenotypes` (phenotype_code, description) VALUES (:phenotype_code, :description)"
    for phenotype_code, description in phenotypes:
        conn.execute(text(insert_query), {'phenotype_code': phenotype_code, 'description': description})
    
    conn.commit()


def downgrade() -> None:
    conn = op.get_bind()
    
    # Restore previous data (ICD-10-like codes with K suffix)
    conn.execute(text("DELETE FROM `sgc_phenotypes`"))
    
    # Restore the old phenotype data
    old_phenotypes = [
        ("L01.0", "Impetigo, unspecified"),
        ("L02", "Cutaneous abscess, furuncle and carbuncle"),
        ("L03", "Cellulitis and acute lymphangitis"),
        ("L05", "Pilonidal cyst and sinus"),
        ("L05.0", "Pilonidal cyst with abscess"),
        ("L05.9", "Pilonidal cyst without abscess"),
        ("L08", "Other local infections of skin and subcutaneous tissue"),
        ("L10", "Pemphigus"),
        ("L10.0", "Pemphigus vulgaris"),
        ("L10.2", "Pemphigus foliaceus"),
        ("L12", "Pemphigoid"),
        ("L12.0", "Bullous pemphigoid"),
        ("L13.0", "Dermatitis herpetiformis"),
        ("L20", "Atopic dermatitis"),
        ("L21", "Seborrheic dermatitis"),
        ("L23_L24_L25", "Contact dermatitis (allergic, irritant, unspecified)"),
        ("L23", "Allergic contact dermatitis"),
        ("L24", "Irritant contact dermatitis"),
        ("L27", "Dermatitis due to substances taken internally"),
        ("L28", "Lichen simplex chronicus and prurigo"),
        ("L28.0", "Lichen simplex chronicus"),
        ("L28.1", "Prurigo nodularis"),
        ("L29", "Pruritus"),
        ("L30", "Other and unspecified dermatitis"),
        ("L40", "Psoriasis"),
        ("L40.0", "Psoriasis vulgaris"),
        ("L40.1", "Generalized pustular psoriasis"),
        ("L40.3", "Pustulosis of palms and soles"),
        ("L40.4", "Guttate psoriasis"),
        ("L40.5", "Arthropathic psoriasis"),
        ("L43", "Lichen planus"),
        ("L50", "Urticaria"),
        ("L51.0", "Nonbullous erythema multiforme"),
        ("L51.1_L51.2_L51.3", "Stevens-Johnson syndrome and toxic epidermal necrolysis"),
        ("L51.8_L51.9", "Other and unspecified erythema multiforme"),
        ("L52", "Erythema nodosum"),
        ("L53.9", "Erythematous condition, unspecified"),
        ("L66", "Cicatricial alopecia"),
        ("L68.0", "Hirsutism"),
        ("L70", "Acne"),
        ("L71", "Rosacea"),
        ("L72", "Follicular cysts of skin and subcutaneous tissue"),
        ("L72.0", "Epidermal cyst"),
        ("L72.1", "Pilar and trichilemmal cyst"),
        ("L73.0", "Acne keloid"),
        ("L73.2", "Hidradenitis suppurativa"),
        ("L74.4", "Anhidrosis"),
        ("L75.0", "Bromhidrosis"),
        ("L80", "Vitiligo"),
        ("L81.2", "Freckles"),
        ("L81.3", "Café au lait spots"),
        ("L81.4", "Other melanin hyperpigmentation"),
        ("L82", "Seborrheic keratosis"),
        ("L83", "Acanthosis nigricans"),
        ("L84", "Corns and callosities"),
        ("L85", "Other epidermal thickening"),
        ("L88", "Pyoderma gangrenosum"),
        ("L89", "Pressure ulcer and chronic ulcer of skin"),
        ("L90.0", "Lichen sclerosus et atrophicus"),
        ("L90.5", "Scar conditions and fibrosis of skin"),
        ("L91.0", "Hypertrophic scar"),
        ("L91.8_L91.9", "Other and unspecified hypertrophic and atrophic conditions of skin"),
        ("L92", "Granulomatous disorders of skin and subcutaneous tissue"),
        ("L92.0", "Granuloma annulare"),
        ("L92.1", "Necrobiosis lipoidica"),
        ("L93", "Lupus erythematosus"),
        ("L93.0", "Discoid lupus erythematosus"),
        ("L93.1", "Subacute cutaneous lupus erythematosus"),
        ("L94.0_L94.1", "Localized and linear scleroderma"),
        ("L94.2", "Calcinosis cutis"),
        ("L94.3", "Sclerodactyly"),
        ("L94.4", "Gottron papules"),
        ("L95", "Vasculitis limited to skin"),
        ("L97_L98.4", "Ulcer and chronic skin breakdown"),
        ("L98.0", "Pyogenic granuloma"),
        ("L98.2", "Febrile neutrophilic dermatosis [Sweet syndrome]"),
        ("L99.0", "Amyloidosis of skin"),
        ("C43", "Malignant melanoma of skin"),
        ("C44", "Other and unspecified malignant neoplasm of skin"),
        ("C44.X1", "Basal cell carcinoma of skin"),
        ("C44.X2", "Squamous cell carcinoma of skin"),
        ("D03", "Melanoma in situ"),
        ("D86.3", "Sarcoidosis of skin"),
        ("M33", "Dermatopolymyositis"),
        ("Q80.0", "Ichthyosis vulgaris"),
        ("A63.0", "Anogenital (venereal) warts"),
        ("B00", "Herpesviral [herpes simplex] infections"),
        ("B02", "Zoster [herpes zoster]"),
        ("B35_B36.1", "Dermatophytosis and superficial mycoses"),
        ("B86", "Scabies"),
        ("D72.12", "Eosinophilia"),
        ("O26.4", "Herpes gestationis"),
        ("O26.86", "Pruritic urticarial papules and plaques of pregnancy")
    ]
    
    insert_query = "INSERT INTO `sgc_phenotypes` (phenotype_code, description) VALUES (:phenotype_code, :description)"
    for phenotype_code, description in old_phenotypes:
        conn.execute(text(insert_query), {'phenotype_code': phenotype_code, 'description': description})
    
    conn.commit()
