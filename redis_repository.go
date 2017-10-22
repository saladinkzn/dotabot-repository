package repository

import (
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

const keyPrefix = "telegramMatchSubscription:"
const accountPrefix = "lastMatches.["

type RedisRepository struct {
	client *redis.Client
}

func CreateRedisRepository(client *redis.Client) SubscriptionRepository {
	return RedisRepository{
		client: client,
	}
}

func parseDotaAccountId(k string) string {
	return k[len(accountPrefix) : len(k)-1]
}

func (this RedisRepository) GetLastKnownMatchId(subscription TelegramMatchSubscription) (result int64, err error) {
	// Supporting spring-data-redis style
	hash, err := this.client.HGetAll(keyPrefix + strconv.FormatInt(subscription.ChatId, 10)).Result()
	if err != nil {
		return
	}

	for k, v := range hash {
		if !strings.HasPrefix(k, accountPrefix) {
			continue
		}

		substr := parseDotaAccountId(k)
		if subscription.DotaAccountId != substr {
			continue
		}

		result, err = strconv.ParseInt(v, 10, 64)
		return
	}

	result = -1
	return
}

func (this RedisRepository) SaveLastKnownMatchId(subscription TelegramMatchSubscription, matchId uint64) error {
	telegramChatKey := keyPrefix + strconv.FormatInt(subscription.ChatId, 10)

	_, err := this.client.HSet(telegramChatKey, "lastMatches.["+subscription.DotaAccountId+"]", strconv.FormatUint(matchId, 10)).Result()
	return err
}

func (this RedisRepository) FindAll() (result []TelegramMatchSubscription, err error) {
	chatIds, err := this.client.Keys(keyPrefix + "*").Result()
	if err != nil {
		return
	}

	for _, chatIdKey := range chatIds {
		chatId, err := strconv.ParseInt(chatIdKey[len(keyPrefix):len(chatIdKey)], 10, 64)
		if err != nil {
			return nil, err
		}

		subscriptions, err := this.findSubscriptions(chatId, chatIdKey)
		if err != nil {
			return nil, err
		}
		result = append(result, subscriptions...)
	}

	return
}

func (this RedisRepository) FindByChatId(chatId int64) (result []TelegramMatchSubscription, err error) {
	hash, err := this.client.HGetAll(keyPrefix + strconv.FormatInt(chatId, 10)).Result()
	if err != nil {
		return
	}

	for k, _ := range hash {
		if !strings.HasPrefix(k, accountPrefix) {
			continue
		}

		accountId := parseDotaAccountId(k)
		result = append(result, TelegramMatchSubscription{ChatId: chatId, DotaAccountId: accountId})
	}

	return
}

func (this RedisRepository) findSubscriptions(chatId int64, chatIdKey string) (result []TelegramMatchSubscription, err error) {
	hkeys, err := this.client.HKeys(chatIdKey).Result()
	if err != nil {
		return nil, err
	}

	for _, hkey := range hkeys {
		if !strings.HasPrefix(hkey, accountPrefix) {
			continue
		}

		dotaAccountId := parseDotaAccountId(hkey)
		subscription := TelegramMatchSubscription{ChatId: chatId, DotaAccountId: dotaAccountId}
		result = append(result, subscription)
	}

	return
}
