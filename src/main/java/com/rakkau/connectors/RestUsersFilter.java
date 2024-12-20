package com.rakkau.connectors;

public class RestUsersFilter
{
	public String byUsername;
    public String byUid;
    public String byEmail;
    public String byName;

    @Override
    public String toString() {
        return "UserFilter{" +
                "byUsername='" + byUsername + '\'' +
                ", byUid=" + byUid +
                '}';
    }
	
}
