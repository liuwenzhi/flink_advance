package cn.supcon.entity;

public class Event {

    private String name;
    private String url;
    private Long money;

    public Event(String name, String url, Long money) {
        this.name = name;
        this.url = url;
        this.money = money;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getMoney() {
        return money;
    }

    public void setMoney(Long money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", money=" + money +
                '}';
    }
}
