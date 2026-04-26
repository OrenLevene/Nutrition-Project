# NutriShop Product Requirements Document (PRD)

## Vision Statement

**NutriShop** is a **free**, intelligent grocery shopping assistant that generates scientifically-optimized weekly shopping lists tailored to **family nutritional needs**, dietary preferences, UK supermarket availability, and **budget optimization with cross-store price comparison**.

> [!IMPORTANT]
> **Core Principle: Always free for users.**
> An app about saving money shouldn't cost money. Revenue comes from supermarket affiliate commissions, not user subscriptions.

---

## The 5 Pillars

| # | Pillar | Description | Implementation Status |
|---|--------|-------------|----------------------|
| 1 | 🥗 **Nutrition** | Complete RDA/UL optimization using MILP | ✅ Core built |
| 2 | 💷 **Cost** | Cross-supermarket price comparison, budget optimization | 🔜 Planned |
| 3 | ⏱️ **Time** | Quick meal planning, easy swaps, one-tap shopping lists | 🔜 Planned |
| 4 | 🎯 **Personalization** | Per-family-member dietary profiles, picky eater tracking | 🔜 Planned |
| 5 | 🛒 **Seamless Shopping** | Auto-generated lists → UK supermarket basket integration | 🔜 Planned |

---

## Target Users

### Primary: Budget-Conscious Families
- Parents who want healthy meals but need to watch spending
- Families with picky eaters or multiple dietary requirements
- Time-poor households who want planning taken care of

### Secondary: Health-Focused Individuals
- People with specific nutritional goals (muscle building, weight loss)
- Those with dietary restrictions (vegan, gluten-free, allergies)

---

## Detailed Feature Specifications

### 1. Complete Nutritional Optimization
**Goal:** Generate shopping lists that satisfy ALL nutritional requirements with no deficiencies.

| Requirement | Description |
|-------------|-------------|
| Caloric Targets | Meet daily caloric needs based on TDEE calculations |
| Macronutrients | Protein, carbohydrates, fats within healthy AMDR ranges |
| Micronutrients | All vitamins (A, B-complex, C, D, E, K) at RDA/AI levels |
| Minerals | Iron, calcium, zinc, magnesium, potassium, etc. |
| Upper Limits | Respect Tolerable Upper Intake Levels (UL) to prevent toxicity |
| Fiber & Water | Adequate fiber intake and hydration considerations |

**Implementation:** Mathematical optimization with constraints for minimum RDA and maximum UL values.

---

### 2. Local Store Integration
**Goal:** Only recommend products actually available at the user's local supermarket in purchasable quantities.

| Requirement | Description |
|-------------|-------------|
| Store Catalog Sync | Integration with UK supermarket APIs (Tesco, Sainsbury's, etc.) |
| Real-time Availability | Check product stock before recommending |
| Discrete Portions | Recommend whole units (1 pack, 2 cans) not fractional amounts |
| Package Sizes | Account for actual package sizes (500g bag, 6-pack) |
| Location-based | Filter to user's nearest store or delivery area |

**Implementation:** Store API integrations, portion size configurations, integer constraints for discrete quantities.

---

### 3. Price-Aware Optimization (KEY DIFFERENTIATOR)
**Goal:** Minimize cost while meeting nutritional requirements, with cross-supermarket comparison.

| Requirement | Description |
|-------------|-------------|
| Budget Constraints | User can set weekly/monthly budget limits |
| Price Optimization | Objective function minimizes total cost |
| **Cross-Store Comparison** | Compare basket cost across Tesco, Asda, Sainsbury's, Lidl, Aldi |
| Value Analysis | Cost-per-nutrient efficiency rankings |
| Deal Integration | Factor in current offers and loyalty card prices |
| Price History | Track prices for smart timing suggestions |

**Implementation:** Cost coefficients in objective function, budget as inequality constraint, price data from Trolley.co.uk or store APIs.

---

### 4. Goal-Based Recommendations
**Goal:** Tailor nutrition to specific health and fitness objectives with scientifically-backed guidance.

| Goal | Nutritional Adjustments |
|------|------------------------|
| **Weight Loss** | Caloric deficit (10-20%), higher protein (1.6-2.2g/kg), adequate fiber |
| **Muscle Building** | Caloric surplus (10-20%), high protein (1.6-2.2g/kg), creatine support |
| **Maintenance** | TDEE-matched calories, balanced macros within AMDR |
| **Athletic Performance** | Higher carbs, timing optimization, electrolyte focus |
| **General Health** | Balanced approach with emphasis on micronutrient diversity |

**Implementation:** Goal-specific constraint modifiers, evidence-based multipliers from sports nutrition research.

> [!NOTE]
> All recommendations must be based on peer-reviewed scientific literature. The system should cite sources and never make unsubstantiated health claims.

---

### 5. Quick Product Switches
**Goal:** Allow easy product substitutions where nutritional impact is minimal.

| Requirement | Description |
|-------------|-------------|
| Similarity Detection | Identify nutritionally-equivalent alternatives |
| One-Click Swaps | Easy UI to swap e.g., "Salmon → Haddock" |
| Impact Preview | Show how swap affects nutrition before confirming |
| Category Grouping | Group switchable items (white fish, leafy greens, etc.) |
| Preference Memory | Remember switches for future recommendations |

**Implementation:** Nutritional similarity scoring (cosine similarity on nutrient vectors), constraint re-validation on swap.

---

### 6. Adaptive Learning System
**Goal:** Learn user preferences to reduce manual adjustments over time.

| Requirement | Description |
|-------------|-------------|
| Swap Tracking | Record every product swap the user makes |
| Preference Modeling | Build user taste profile from choices |
| Diversity Injection | Ensure variety while respecting preferences |
| Feedback Loop | Explicit ratings and implicit behavior signals |
| Cold Start | Sensible defaults for new users with preference questionnaire |

**Implementation:** Collaborative filtering, preference embeddings, reinforcement learning for long-term satisfaction.

---

### 7. Dietary Requirement Filters
**Goal:** Accommodate all major dietary restrictions and preferences.

| Filter | Foods Excluded |
|--------|---------------|
| **Vegetarian** | Meat, poultry, fish (allows dairy, eggs) |
| **Vegan** | All animal products |
| **Pescatarian** | Meat, poultry (allows fish) |
| **Kosher** | Non-kosher meats, shellfish, mixing meat/dairy |
| **Halal** | Pork, non-halal meats, alcohol |
| **Gluten-Free** | Wheat, barley, rye, contaminated oats |
| **Lactose-Free** | Milk, cheese, cream (allows lactose-free alternatives) |
| **Nut-Free** | All tree nuts and peanuts |
| **Low-Sodium** | High-sodium processed foods |

**Implementation:** Filter constraints in optimizer, food database tagging, certification verification.

---

## Core Features (Phased)

### Phase 1: Foundation (Current State)
*Status: Largely complete*

| Feature | Description | Status |
|---------|-------------|--------|
| Nutrition Database | FooDB + CoFID + OFF merged, ~42k UK products | ✅ Complete |
| MILP Optimizer | Complete RDA/UL optimization with tiered tolerances | ✅ Complete |
| Genetic Algorithm | Multi-day meal planning with variety scoring | ✅ Complete |
| Food Filtering | Exclude industrial/exotic foods, focus on purchasables | ✅ Complete |
| Basic Web Interface | Flask-based calculator and optimizer UI | ✅ Complete |
| Product Matching | Canonical foods → OFF store products | ✅ Complete |

---

### Phase 2: Family Profiles & Personalization
*Status: Planned*

| Feature | Description | Priority |
|---------|-------------|----------|
| **Household Profiles** | Create household with multiple family members | High |
| **Per-Person Dietary Needs** | Age, gender, activity level, goals per person | High |
| **Dietary Restrictions** | Allergies, intolerances, preferences per person | High |
| **Picky Eater Tracking** | Track foods each family member likes/dislikes | Medium |
| **Combined Shopping List** | Aggregate optimization across all household members | High |
| **Overlap Detection** | Find meals everyone in household can eat | Medium |

**User Story:**
> As a parent, I want to set up profiles for my family (me, partner, 2 kids with different preferences) so that NutriShop generates a single optimized shopping list that meets everyone's needs.

**Technical Approach:**
- Extend user model with `Household` → `Member[]` relationship
- Each member has own nutritional targets (calculate from age/gender/activity)
- Optimizer runs per-member then aggregates quantities
- Filter constraints applied per-member (allergies, exclusions)

---

### Phase 3: Cost Optimization & Price Comparison
*Status: Planned - KEY DIFFERENTIATOR*

| Feature | Description | Priority |
|---------|-------------|----------|
| **Multi-Supermarket Price Data** | Integrate Tesco, Asda, Sainsbury's, Lidl, Aldi pricing | Critical |
| **Cross-Store Comparison** | Show which store has cheapest price for each item | Critical |
| **Basket Optimization** | "Cheapest basket at single store" vs "split across stores" | High |
| **Budget Constraints** | Set weekly/monthly budget limit | High |
| **Price History** | Track price trends, suggest optimal timing | Medium |
| **Deal Integration** | Incorporate promotions and loyalty card prices | Medium |

**User Story:**
> As a budget-conscious parent, I want to see that my shopping list costs £52 at Tesco, £48 at Sainsbury's, or £45 split across Aldi+Tesco, so I can choose the best option for my situation.

**Technical Approach:**
- Price data source: Trolley.co.uk API OR direct store scraping
- Extend MILP objective function with price coefficients
- Generate alternative baskets (single-store vs multi-store)
- Cache prices with TTL, refresh daily

**Data Sources:**
| Source | Coverage | Method |
|--------|----------|--------|
| Trolley.co.uk | Tesco, Asda, Sainsbury's, Aldi, Lidl, Waitrose | API partnership or scraping |
| Store APIs | Tesco (Clubcard), Sainsbury's (Nectar) | Official affiliate programs |
| Open Food Facts | Some price data | Already integrated |

---

### Phase 4: Seamless Shopping Integration
*Status: Planned*

| Feature | Description | Priority |
|---------|-------------|----------|
| **One-Tap Add to Basket** | Deep link to Tesco/Asda/Sainsbury's app with pre-filled basket | High |
| **Smart Quantities** | Calculate exact pack sizes needed (2x 500g, not 1.2kg) | High |
| **Substitution Suggestions** | If product unavailable, suggest alternatives | Medium |
| **Availability Checking** | Verify products in stock before finalizing | Medium |
| **Order History** | Track past shopping lists for quick reordering | Low |

**User Story:**
> As a time-poor parent, I want to tap "Order from Tesco" and have my entire shopping list added to my Tesco basket, ready to checkout.

**Technical Approach:**
- Deep linking to supermarket apps (affiliate model)
- Pre-populate basket via URL parameters where supported
- Fallback: exportable shopping list (organized by store aisle)

---

### Phase 5: Recipe Integration & Meal Planning
*Status: Planned*

| Feature | Description | Priority |
|---------|-------------|----------|
| **Recipe Suggestions** | Suggest recipes using optimized shopping list | Medium |
| **Quick Meal Filters** | Under 30 min, under 5 ingredients, kid-friendly | Medium |
| **Weekly Meal Calendar** | Visual weekly plan with drag-and-drop | Medium |
| **Prep Time Optimization** | Batch cooking suggestions, leftover utilization | Low |
| **Recipe Import** | Import from URLs, extract ingredients | Low |

---

## Monetization Strategy

### Revenue Model: 100% Free for Users

```
┌─────────────────────────────────────────────────────────────────┐
│                     💷 REVENUE SOURCES                          │
│                     (User pays nothing)                         │
│                                                                 │
│   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────┐ │
│   │  🛒 Affiliate   │   │  🥦 Sponsored   │   │  📊 B2B      │ │
│   │  Commissions    │   │  Brands         │   │  Licensing   │ │
│   │  1-4% on orders │   │  (ethical only) │   │  (future)    │ │
│   └─────────────────┘   └─────────────────┘   └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Affiliate Commission Rates (UK)
| Supermarket | Commission |
|-------------|------------|
| Tesco | 0.8% - 3.2% |
| Asda | 1% |
| Ocado (new customer) | 3% |
| Sainsbury's | TBD |

### Ethical Guardrails
- ✅ Always show true price comparison (never favor partners)
- ✅ Be transparent: "We may earn a small commission"
- ✅ Let users choose any store (even without affiliate deals)
- ✅ Never let commission influence nutritional recommendations
- ✅ No banner ads or intrusive monetization

---

## Competitive Differentiation

### vs CherryPick (Closest Competitor)

| Capability | CherryPick | NutriShop |
|------------|------------|-----------|
| Meal planning | ✅ Recipe-based | ✅ Nutrition-optimized |
| Supermarket integration | ✅ Tesco, Asda, Sainsbury's | 🔜 Same + Lidl, Aldi |
| Price comparison | ❌ Single store per order | ✅ **Cross-store comparison** |
| Nutrition optimization | ❌ Macro-focused | ✅ **Complete RDA/UL (MILP)** |
| Family profiles | ⚠️ Basic exclusions | ✅ **Per-person profiles** |
| Pricing model | Freemium with premium tiers | ✅ **100% free** |
| Budget focus | Health-first | ✅ **Budget-first + health** |

### Unique Value Proposition
*"The budget-conscious family's meal planner — complete nutrition optimized across UK supermarkets, always free."*

---

## Success Metrics

| Metric | Target | Rationale |
|--------|--------|-----------|
| Weekly Active Users | 70%+ retention | Habit formation indicator |
| Meal Plans Created/Week | 1+ per household | Core value delivery |
| Cross-Store Comparison Usage | 50%+ of users | Differentiator adoption |
| Shopping List → Order Rate | 30%+ | Affiliate revenue enabler |
| Nutritional Completeness | 100% RDA met | Scientific credibility |
| User Satisfaction (NPS) | 50+ | Word-of-mouth growth |

---

## Technical Architecture (Current)

```
src/
├── calculator/       # TDEE/BMR calculations
├── ingestion/        # Data pipelines (OFF, FooDB, CoFID)
├── optimizer/        # MILP + Genetic Algorithm
├── utils/            # Food filtering, helpers
└── web_app/          # Flask frontend
```

### Planned Extensions
| Component | Purpose |
|-----------|---------|
| `src/users/` | Household and member profile management |
| `src/pricing/` | Price data ingestion, comparison logic |
| `src/shopping/` | Basket generation, deep linking |
| `src/recipes/` | Recipe matching and suggestions |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Price data freshness | High | Medium | Multiple sources, cache with TTL, user reporting |
| Supermarket API changes | Medium | High | Abstract API layer, fallback to manual lists |
| CherryPick adding price comparison | Medium | Medium | Move fast, build community, stay free |
| Low affiliate revenue at small scale | High | Medium | Keep costs minimal, treat as passion project initially |
| Family profile complexity | Medium | Medium | Start with 2-4 person households, iterate |

---

## Next Steps

### Immediate (This Week)
1. [x] Merged MISSION.md and refined 5-pillar vision
2. [ ] Research Trolley.co.uk API or alternative price sources
3. [ ] Design household/member database schema

### Short-Term (Next Month)
4. [ ] Implement basic household profile management
5. [ ] Add per-member dietary restrictions to optimizer
6. [ ] Prototype price data integration

### Medium-Term (3 Months)
7. [ ] Build cross-store price comparison UI
8. [ ] Implement supermarket deep linking
9. [ ] Add recipe suggestions based on shopping list

