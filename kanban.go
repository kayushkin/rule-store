package rulestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// KanbanStore reads the two facts a session rule may need: the cards linked
// to a session, with where each sits, and the session's entity tags. It calls
// as the service, because entity tags answer 403 to a principal.
type KanbanStore struct {
	BaseURL      string
	ServiceToken string
	Client       *http.Client
}

// NewKanbanStore builds the reader. Both values are required settings.
func NewKanbanStore(baseURL, serviceToken string) *KanbanStore {
	return &KanbanStore{BaseURL: baseURL, ServiceToken: serviceToken, Client: &http.Client{Timeout: 20 * time.Second}}
}

// errKanbanNotFound is a 404 from kanban-store.
var errKanbanNotFound = errors.New("kanban-store: not found")

func (k *KanbanStore) getJSON(ctx context.Context, path string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, k.BaseURL+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Kanban-Store-Service-Token", k.ServiceToken)
	response, err := k.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: GET %s: %.300s", errKanbanNotFound, path, body)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("kanban-store GET %s: %d %.300s", path, response.StatusCode, body)
	}
	return json.Unmarshal(body, target)
}

// LinkedCardPlacements returns every placement of every card linked to the
// session. A card on two boards is two placements.
func (k *KanbanStore) LinkedCardPlacements(ctx context.Context, sessionID string) ([]CardPlacement, error) {
	var linked []struct {
		CardID string `json:"card_id"`
	}
	if err := k.getJSON(ctx, "/api/entities/session/"+url.PathEscape(sessionID)+"/cards", &linked); err != nil {
		return nil, err
	}
	placements := []CardPlacement{}
	for _, l := range linked {
		var card struct {
			Placements []struct {
				BoardID  string `json:"board_id"`
				ColumnID string `json:"column_id"`
			} `json:"placements"`
		}
		err := k.getJSON(ctx, "/api/cards/"+url.PathEscape(l.CardID), &card)
		// kanban-store answers 404 for a card that is on no board: linked,
		// but detached from every board. It sits in no column, so it has no
		// placement, which is an answer, not a failure.
		if errors.Is(err, errKanbanNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, p := range card.Placements {
			placements = append(placements, CardPlacement{CardID: l.CardID, BoardID: p.BoardID, ColumnID: p.ColumnID})
		}
	}
	return placements, nil
}

// SessionTags returns the session's entity tags. kanban-store answers null
// for an entity with none; that is an empty list, not an error.
func (k *KanbanStore) SessionTags(ctx context.Context, sessionID string) ([]string, error) {
	var tags []struct {
		Tag string `json:"tag"`
	}
	if err := k.getJSON(ctx, "/api/entities/session/"+url.PathEscape(sessionID)+"/tags", &tags); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(tags))
	for _, t := range tags {
		names = append(names, t.Tag)
	}
	return names, nil
}

// CardPlacementDetail is where a card sits on one board, and when that
// placement last changed.
type CardPlacementDetail struct {
	BoardID   string `json:"board_id"`
	ColumnID  string `json:"column_id"`
	UpdatedAt string `json:"updated_at"`
}

// CardDetail is the part of GET /api/cards/{id} a comparison reads.
type CardDetail struct {
	Placements []CardPlacementDetail `json:"placements"`
	Links      []struct {
		EntityType string `json:"entity_type"`
		EntityRef  string `json:"entity_ref"`
	} `json:"links"`
}

// Card reads one card's placements and links.
func (k *KanbanStore) Card(ctx context.Context, cardID string) (CardDetail, error) {
	var card CardDetail
	err := k.getJSON(ctx, "/api/cards/"+url.PathEscape(cardID), &card)
	return card, err
}

// ColumnArrival is a card in a column, and when it arrived there.
type ColumnArrival struct {
	CardID    string `json:"card_id"`
	ArrivedAt string `json:"arrived_at"`
}

// ColumnCards lists every card in one column of a board, with no limit, so a
// comparison counts every card the old binary moved there.
func (k *KanbanStore) ColumnCards(ctx context.Context, boardID, columnID string) ([]ColumnArrival, error) {
	var view struct {
		Columns []struct {
			Column struct {
				ID string `json:"id"`
			} `json:"column"`
			Cards []struct {
				Item struct {
					ID string `json:"id"`
				} `json:"item"`
				Placement struct {
					UpdatedAt string `json:"updated_at"`
				} `json:"placement"`
			} `json:"cards"`
		} `json:"columns"`
	}
	if err := k.getJSON(ctx, "/api/boards/"+url.PathEscape(boardID)+"/cards", &view); err != nil {
		return nil, err
	}
	for _, column := range view.Columns {
		if column.Column.ID != columnID {
			continue
		}
		arrivals := make([]ColumnArrival, 0, len(column.Cards))
		for _, card := range column.Cards {
			arrivals = append(arrivals, ColumnArrival{CardID: card.Item.ID, ArrivedAt: card.Placement.UpdatedAt})
		}
		return arrivals, nil
	}
	return nil, fmt.Errorf("board %s has no column %s", boardID, columnID)
}
