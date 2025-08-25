"""sgc_phenotypes_table

Revision ID: abc123def456
Revises: d2ebd3a31541
Create Date: 2025-02-08 14:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'abc123def456'
down_revision = 'd2ebd3a31541'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Create table
    query = """
        CREATE TABLE `sgc_phenotypes` (
        `phenotype_code` varchar(50) NOT NULL,
        `description` text NOT NULL,
        `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`phenotype_code`)
        )
        """
    conn.execute(text(query))
    
    # Insert phenotype data
    phenotypes = [
        ("L01.0K", "Impetigo, unspecified"),
        ("L02K", "Cutaneous abscess, furuncle and carbuncle"),
        ("L03K", "Cellulitis and acute lymphangitis"),
        ("L05K", "Pilonidal cyst and sinus"),
        ("L05.0K", "Pilonidal cyst with abscess"),
        ("L05.9K", "Pilonidal cyst without abscess"),
        ("L08K", "Other local infections of skin and subcutaneous tissue"),
        ("L10K", "Pemphigus"),
        ("L10.0K", "Pemphigus vulgaris"),
        ("L10.2K", "Pemphigus foliaceus"),
        ("L12K", "Pemphigoid"),
        ("L12.0K", "Bullous pemphigoid"),
        ("L13.0K", "Dermatitis herpetiformis"),
        ("L20K", "Atopic dermatitis"),
        ("L21K", "Seborrheic dermatitis"),
        ("L23_L24_L25K", "Contact dermatitis (allergic, irritant, unspecified)"),
        ("L23K", "Allergic contact dermatitis"),
        ("L24K", "Irritant contact dermatitis"),
        ("L27K", "Dermatitis due to substances taken internally"),
        ("L28K", "Lichen simplex chronicus and prurigo"),
        ("L28.0K", "Lichen simplex chronicus"),
        ("L28.1K", "Prurigo nodularis"),
        ("L29K", "Pruritus"),
        ("L30K", "Other and unspecified dermatitis"),
        ("L40K", "Psoriasis"),
        ("L40.0K", "Psoriasis vulgaris"),
        ("L40.1K", "Generalized pustular psoriasis"),
        ("L40.3K", "Pustulosis of palms and soles"),
        ("L40.4K", "Guttate psoriasis"),
        ("L40.5K", "Arthropathic psoriasis"),
        ("L43K", "Lichen planus"),
        ("L50K", "Urticaria"),
        ("L51.0K", "Nonbullous erythema multiforme"),
        ("L51.1_L51.2_L51.3K", "Stevens-Johnson syndrome and toxic epidermal necrolysis"),
        ("L51.8_L51.9K", "Other and unspecified erythema multiforme"),
        ("L52K", "Erythema nodosum"),
        ("L53.9K", "Erythematous condition, unspecified"),
        ("L66K", "Cicatricial alopecia"),
        ("L68.0K", "Hirsutism"),
        ("L70K", "Acne"),
        ("L71K", "Rosacea"),
        ("L72K", "Follicular cysts of skin and subcutaneous tissue"),
        ("L72.0K", "Epidermal cyst"),
        ("L72.1K", "Pilar and trichilemmal cyst"),
        ("L73.0K", "Acne keloid"),
        ("L73.2K", "Hidradenitis suppurativa"),
        ("L74.4K", "Anhidrosis"),
        ("L75.0K", "Bromhidrosis"),
        ("L80K", "Vitiligo"),
        ("L81.2K", "Freckles"),
        ("L81.3K", "Café au lait spots"),
        ("L81.4K", "Other melanin hyperpigmentation"),
        ("L82K", "Seborrheic keratosis"),
        ("L83K", "Acanthosis nigricans"),
        ("L84K", "Corns and callosities"),
        ("L85K", "Other epidermal thickening"),
        ("L88K", "Pyoderma gangrenosum"),
        ("L89K", "Pressure ulcer and chronic ulcer of skin"),
        ("L90.0K", "Lichen sclerosus et atrophicus"),
        ("L90.5K", "Scar conditions and fibrosis of skin"),
        ("L91.0K", "Hypertrophic scar"),
        ("L91.8_L91.9K", "Other and unspecified hypertrophic and atrophic conditions of skin"),
        ("L92K", "Granulomatous disorders of skin and subcutaneous tissue"),
        ("L92.0K", "Granuloma annulare"),
        ("L92.1K", "Necrobiosis lipoidica"),
        ("L93K", "Lupus erythematosus"),
        ("L93.0K", "Discoid lupus erythematosus"),
        ("L93.1K", "Subacute cutaneous lupus erythematosus"),
        ("L94.0_L94.1K", "Localized and linear scleroderma"),
        ("L94.2K", "Calcinosis cutis"),
        ("L94.3K", "Sclerodactyly"),
        ("L94.4K", "Gottron papules"),
        ("L95K", "Vasculitis limited to skin"),
        ("L97_L98.4K", "Ulcer and chronic skin breakdown"),
        ("L98.0K", "Pyogenic granuloma"),
        ("L98.2K", "Febrile neutrophilic dermatosis [Sweet syndrome]"),
        ("L99.0K", "Amyloidosis of skin"),
        ("C43K", "Malignant melanoma of skin"),
        ("C44K", "Other and unspecified malignant neoplasm of skin"),
        ("C44.X1K", "Basal cell carcinoma of skin"),
        ("C44.X2K", "Squamous cell carcinoma of skin"),
        ("D03K", "Melanoma in situ"),
        ("D86.3K", "Sarcoidosis of skin"),
        ("M33K", "Dermatopolymyositis"),
        ("Q80.0K", "Ichthyosis vulgaris"),
        ("A63.0K", "Anogenital (venereal) warts"),
        ("B00K", "Herpesviral [herpes simplex] infections"),
        ("B02K", "Zoster [herpes zoster]"),
        ("B35_B36.1K", "Dermatophytosis and superficial mycoses"),
        ("B86K", "Scabies"),
        ("D72.12K", "Eosinophilia"),
        ("O26.4K", "Herpes gestationis"),
        ("O26.86K", "Pruritic urticarial papules and plaques of pregnancy")
    ]
    
    insert_query = "INSERT INTO `sgc_phenotypes` (phenotype_code, description) VALUES (:phenotype_code, :description)"
    for phenotype_code, description in phenotypes:
        conn.execute(text(insert_query), {'phenotype_code': phenotype_code, 'description': description})


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_phenotypes`"))
